import requests
import pandas as pd
import zipfile
import io
from datetime import datetime
import signal
import sys
import os  # Make sure os is imported
from tqdm import tqdm
from config_reader import ConfigReader
from concurrent.futures import ThreadPoolExecutor, as_completed

class DB1BCouponDownloader:
    def __init__(self, config: ConfigReader):
        self.config = config
        self.session = requests.Session()
        self.should_exit = False
        
        # Ensure data directories exist
        self.data_dir = "app/server/data/coupon"
        os.makedirs(self.data_dir, exist_ok=True)
        
        # Rest of initialization...
        if 'db1b_coupon' not in self.config.config:
            raise ValueError("Configuration missing db1b_coupon section")
        self.config.config['db1b_config'] = self.config.config['db1b_coupon']
        
        signal.signal(signal.SIGINT, self.handle_interrupt)
        signal.signal(signal.SIGTERM, self.handle_interrupt)
        print("Press Ctrl+C at any time to stop processing...")

    def handle_interrupt(self, signum, frame):
        print("\nReceived interrupt signal. Cleaning up...")
        self.should_exit = True

    def download_with_progress(self, url: str) -> bytes:
        download_config = self.config.download_config
        response = self.session.get(
            url, 
            stream=True,
            verify=download_config['verify_ssl'],
            timeout=30
        )
        response.raise_for_status()
        
        total_size = int(response.headers.get('content-length', 0))
        block_size = 8192
        buffer = io.BytesIO()
        
        with tqdm(total=total_size, unit='iB', unit_scale=True, desc="Downloading") as pbar:
            for chunk in response.iter_content(block_size):
                if self.should_exit:
                    raise KeyboardInterrupt()
                buffer.write(chunk)
                pbar.update(len(chunk))
        
        return buffer.getvalue()

    def process_data(self, year: int, quarter: int) -> None:
        url = f"{self.config.base_url}_{year}_{quarter}.zip"
        try:
            print(f"\nProcessing {year} Q{quarter} data...")
            zip_data = self.download_with_progress(url)
            
            print("\nExtracting data...")
            with zipfile.ZipFile(io.BytesIO(zip_data)) as z:
                csv_name = [name for name in z.namelist() if name.endswith('.csv')][0]
                print(f"Found CSV: {csv_name}")
                
                # Create timestamp for consistent file naming
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                
                try:
                    # Read CSV columns we want
                    columns = ['ItinID', 
                               'MktID', 
                               'SeqNum', 
                               'Coupons', 
                               'Origin', 
                               'Dest', 
                               'CouponType', 
                               'TkCarrier', 
                               'OpCarrier', 
                               'RPCarrier', 
                               'Passengers']
                    
                    print("Reading CSV...")
                    chunk_size = 500000  # Process 500k rows at a time
                    chunks = []
                    total_chunks = 0
                    
                    # First pass to count total chunks
                    for _ in pd.read_csv(z.open(csv_name), usecols=columns, chunksize=chunk_size):
                        if self.should_exit:
                            raise KeyboardInterrupt()
                        total_chunks += 1
                    
                    print(f"File will be processed in {total_chunks} chunks")
                    
                    # Second pass to actually read the data with progress
                    for chunk_num, chunk in enumerate(pd.read_csv(z.open(csv_name), 
                                                                usecols=columns, 
                                                                chunksize=chunk_size), 1):
                        if self.should_exit:
                            raise KeyboardInterrupt()
                        chunks.append(chunk)
                        print(f"Read chunk {chunk_num}/{total_chunks} ({(chunk_num/total_chunks*100):.1f}%)")
                    
                    # Combine all chunks
                    print("Combining chunks...")
                    df = pd.concat(chunks, ignore_index=True)
                    print(f"Loaded {len(df):,} records")
                    
                    # Save processed CSV
                    output_file = os.path.join(self.data_dir, 
                                             f"DB1BCoupon_{year}_{quarter}.{timestamp}.csv")
                    print(f"\nSaving to file: {output_file}")
                    df.to_csv(output_file, index=False)
                    print("Processing complete!")
                
                except KeyboardInterrupt:
                    print("\nInterrupt received during processing. Cleaning up...")
                    return
        
        except Exception as e:
            print(f"Error processing {year} Q{quarter}: {str(e)}")
            raise

    def process_all(self):
        pairs = self.config.get_download_pairs()
        max_workers = self.config.download_config['max_concurrent']
        print(f"Starting download of {len(pairs)} quarter(s) with {max_workers} concurrent downloads")
        
        try:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []
                for year, quarter in pairs:
                    if self.should_exit:
                        break
                    futures.append(executor.submit(self.process_data, year, quarter))
                
                for future in as_completed(futures):
                    try:
                        if self.should_exit:
                            executor.shutdown(wait=False, cancel_futures=True)
                            break
                        future.result()
                    except KeyboardInterrupt:
                        print("\nInterrupt received, shutting down...")
                        executor.shutdown(wait=False, cancel_futures=True)
                        return
                    except Exception as e:
                        print(f"Error in download: {str(e)}")
        
        except KeyboardInterrupt:
            print("\nInterrupt received, shutting down gracefully...")
            if 'executor' in locals():
                executor.shutdown(wait=False, cancel_futures=True)
            return
        
        if self.should_exit:
            print("\nProcessing stopped by user.")
            return

if __name__ == "__main__":
    config = ConfigReader()
    downloader = DB1BCouponDownloader(config)
    try:
        downloader.process_all()
    except KeyboardInterrupt:
        sys.exit(1)