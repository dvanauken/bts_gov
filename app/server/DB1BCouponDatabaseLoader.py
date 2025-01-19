import requests
import pandas as pd
import zipfile
import io
import sqlite3
from datetime import datetime, time
import signal
import sys
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from pathlib import Path
from config_reader import ConfigReader
import time

class DB1BCouponDatabaseLoader:
    def __init__(self, config: ConfigReader, db_path='flights.db'):
        self.config = config
        self.session = requests.Session()
        self.should_exit = False
        self.db_path = db_path
        self.setup_logging()
        self.initialize_db()
        
        if 'db1b_coupon' not in self.config.config:
            raise ValueError("Configuration missing db1b_coupon section")
        self.config.config['db1b_config'] = self.config.config['db1b_coupon']
        
        signal.signal(signal.SIGINT, self.handle_interrupt)
        signal.signal(signal.SIGTERM, self.handle_interrupt)
        print("Press Ctrl+C at any time to stop processing...")

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def initialize_db(self):
        """Initialize database with required schema"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Create the main flights table with optimized types
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS flights (
                        year INTEGER NOT NULL,
                        quarter INTEGER NOT NULL CHECK (quarter BETWEEN 1 AND 4),
                        ItinID CHAR(6) NOT NULL,  -- Hex encoded, without year prefix
                        SeqNum INTEGER NOT NULL,
                        Coupons INTEGER NOT NULL,
                        Origin CHAR(3) NOT NULL,   -- Airport codes are always 3 chars
                        Dest CHAR(3) NOT NULL,     -- Airport codes are always 3 chars
                        CouponType CHAR(1) NOT NULL,
                        TkCarrier CHAR(2) NOT NULL,
                        OpCarrier CHAR(2) NOT NULL,
                        RPCarrier CHAR(2) NOT NULL,
                        Passengers REAL NOT NULL,
                        PRIMARY KEY (year, quarter, ItinID, SeqNum)
                    )
                ''')
                
                # Create indices for common query patterns
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_temporal ON flights(year, quarter)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_route ON flights(Origin, Dest)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_carriers ON flights(TkCarrier, OpCarrier)')
                
                self.logger.info("Database initialized successfully")
                
        except sqlite3.Error as e:
            self.logger.error(f"Error initializing database: {e}")
            raise

    def handle_interrupt(self, signum, frame):
        self.logger.info("\nReceived interrupt signal. Cleaning up...")
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

    @staticmethod
    def encode_itin_id(itin_id: str) -> str:
        """Convert numeric ItinID to hex after removing year prefix"""
        try:
            # Extract the unique part (after year)
            unique_part = int(str(itin_id)[4:])
            # Convert to hex and remove '0x' prefix, ensure uppercase
            return hex(unique_part)[2:].upper().zfill(6)
        except (ValueError, IndexError) as e:
            raise ValueError(f"Invalid ItinID format: {itin_id}") from e

    def process_data(self, year: int, quarter: int) -> None:
        url = f"{self.config.base_url}_{year}_{quarter}.zip"
        try:
            self.logger.info(f"\nProcessing {year} Q{quarter} data...")
            zip_data = self.download_with_progress(url)
            self.logger.info("\nExtracting data...")
            
            with zipfile.ZipFile(io.BytesIO(zip_data)) as z:
                csv_name = [name for name in z.namelist() if name.endswith('.csv')][0]
                self.logger.info(f"Found CSV: {csv_name}")
                
                try:
                    columns = ['ItinID', 'SeqNum', 'Coupons', 'Origin', 'Dest', 
                             'CouponType', 'TkCarrier', 'OpCarrier', 'RPCarrier', 'Passengers']
                    
                    # First pass to count total records
                    total_records = 0
                    for chunk in pd.read_csv(z.open(csv_name), usecols=columns, chunksize=500000):
                        total_records += len(chunk)
                    self.logger.info(f"Total records to process: {total_records:,}")
                    
                    processed_records = 0
                    chunk_size = 500000  # Process 500k rows at a time
                    last_update = time.time()
                    
                    with sqlite3.connect(self.db_path) as conn:
                        # Start transaction
                        conn.execute('BEGIN TRANSACTION')
                        
                        # Process each chunk
                        for chunk in pd.read_csv(z.open(csv_name), usecols=columns, chunksize=chunk_size):
                            if self.should_exit:
                                raise KeyboardInterrupt()
                            
                            # Convert ItinID to hex and prepare records
                            records = []
                            for _, row in chunk.iterrows():
                                try:
                                    encoded_itin = self.encode_itin_id(row['ItinID'])
                                    records.append((
                                        year, 
                                        quarter,
                                        encoded_itin,
                                        row['SeqNum'],
                                        row['Coupons'],
                                        row['Origin'],
                                        row['Dest'],
                                        row['CouponType'],
                                        row['TkCarrier'],
                                        row['OpCarrier'],
                                        row['RPCarrier'],
                                        row['Passengers']
                                    ))
                                except ValueError as e:
                                    self.logger.warning(f"Skipping record: {e}")
                                    continue
                            
                            # Insert batch
                            conn.executemany('''
                                INSERT OR REPLACE INTO flights 
                                (year, quarter, ItinID, SeqNum, Coupons,
                                Origin, Dest, CouponType, TkCarrier, OpCarrier,
                                RPCarrier, Passengers)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ''', records)
                            
                            processed_records += len(records)
                            
                            # Update progress every 20 seconds
                            current_time = time.time()
                            if current_time - last_update >= 20:
                                progress = (processed_records / total_records) * 100
                                self.logger.info(f"Processed {processed_records:,} of {total_records:,} records ({progress:.1f}%)")
                                last_update = current_time
                        
                        # Commit transaction
                        conn.commit()
                        
                    self.logger.info(f"\nProcessing complete! Total records processed: {processed_records:,}")
                    
                except KeyboardInterrupt:
                    self.logger.info("\nInterrupt received during processing. Rolling back...")
                    if 'conn' in locals():
                        conn.rollback()
                    return
                    
        except Exception as e:
            self.logger.error(f"Error processing {year} Q{quarter}: {str(e)}")
            raise

    def process_all(self):
        pairs = self.config.get_download_pairs()
        max_workers = self.config.download_config['max_concurrent']
        self.logger.info(f"Starting download of {len(pairs)} quarter(s) with {max_workers} concurrent downloads")
        
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
                        self.logger.info("\nInterrupt received, shutting down...")
                        executor.shutdown(wait=False, cancel_futures=True)
                        return
                    except Exception as e:
                        self.logger.error(f"Error in download: {str(e)}")
                        
        except KeyboardInterrupt:
            self.logger.info("\nInterrupt received, shutting down gracefully...")
            if 'executor' in locals():
                executor.shutdown(wait=False, cancel_futures=True)
            return

if __name__ == "__main__":
    config = ConfigReader()
    loader = DB1BCouponDatabaseLoader(config)
    try:
        loader.process_all()
    except KeyboardInterrupt:
        sys.exit(1)