import requests
import pandas as pd
import zipfile
import io
from datetime import datetime
from tqdm import tqdm
import urllib3
import os
from config_reader import ConfigReader
from concurrent.futures import ThreadPoolExecutor, as_completed

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class DB1BDownloader:
    def __init__(self, config: ConfigReader):
        self.config = config
        self.session = requests.Session()

    def download_with_progress(self, url: str) -> bytes:
        """Download file with progress bar"""
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
                buffer.write(chunk)
                pbar.update(len(chunk))
        
        return buffer.getvalue()

    def process_data(self, year: int, quarter: int) -> None:
        """Download and process a single year-quarter pair"""
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
                
                # Save raw CSV with timestamp
                raw_file = f"data/market/DB1B_MARKET_{year}_{quarter}.{timestamp}.csv"
                with z.open(csv_name) as source, open(raw_file, 'wb') as target:
                    target.write(source.read())
                print(f"Raw CSV saved as: {raw_file}")
                
                # Process the data
                print("Reading CSV...")
                df = pd.read_csv(raw_file)
                print(f"Loaded {len(df):,} records")
                result_df = self.transform_data(df)
                
                # Save summarized data
                output_file = f"data/market/CITY_PAIR_{year}_{quarter}.{timestamp}.txt"

                print(f"Saving processed data to {output_file}")
                
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write("CITYPAIR|OPCR|TKCR|PASSENGERS\n")  # Header row
                    for _, row in result_df.iterrows():
                        f.write(f"{row['CITYPAIR']}|{row['OPCR']}|{row['TKCR']}|{int(row['PASSENGERS'])}\n")
                
                print(f"Processing complete!")
                print(f"\nStatistics:")
                print(f"Total city pairs: {len(result_df['CITYPAIR'].unique()):,}")
                print(f"Total carrier combinations: {len(result_df):,}")
                print(f"Total passengers: {result_df['PASSENGERS'].sum():,}")
                
        except Exception as e:
            print(f"Error processing {year} Q{quarter}: {str(e)}")
            raise

    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform the raw DB1B data into the required format"""
        print("Summarizing market data...")
        
        # Create city pairs (ensuring proper order)
        def make_city_pair(row):
            cities = sorted([str(row['Origin']), str(row['Dest'])])
            return ''.join(cities)
            
        df['CITYPAIR'] = df.apply(make_city_pair, axis=1)
        
        # Clean up carrier codes (handle special cases)
        df['OpCarrier'] = df['OpCarrier'].replace('99', '--')  # Mark unknown operators
        df['TkCarrier'] = df['TkCarrier'].fillna('--')  # Handle any missing ticketing carriers
        
        # Group and sum passengers
        result = df.groupby(
            ['CITYPAIR', 'OpCarrier', 'TkCarrier'],
            as_index=False
        ).agg({
            'Passengers': 'sum'
        })
        
        # Format carrier codes
        result['OPCR'] = result['OpCarrier'].str.strip().str.upper().str.ljust(2)
        result['TKCR'] = result['TkCarrier'].str.strip().str.upper().str.ljust(2)
        
        # Round passengers to integers
        result['PASSENGERS'] = result['Passengers'].round().astype(int)
        
        # Sort by city pair and passengers
        result = result.sort_values(['CITYPAIR', 'PASSENGERS'], ascending=[True, False])
        
        # Select and order final columns
        result = result[['CITYPAIR', 'OPCR', 'TKCR', 'PASSENGERS']]
        
        return result

    def process_all(self):
        """Process all configured year-quarter pairs"""
        pairs = self.config.get_download_pairs()
        max_workers = self.config.download_config['max_concurrent']
        
        print(f"Starting download of {len(pairs)} quarter(s) with {max_workers} concurrent downloads")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(self.process_data, year, quarter)
                for year, quarter in pairs
            ]
            
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Error in download: {str(e)}")

if __name__ == "__main__":
    config = ConfigReader()
    downloader = DB1BDownloader(config)
    downloader.process_all()