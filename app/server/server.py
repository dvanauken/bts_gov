# app/server/server.py
from flask import Flask, send_from_directory, jsonify, request
import sqlite3
import os
import logging

app = Flask(__name__)

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = app.logger

# Directory setup
SERVER_DIR = os.path.dirname(os.path.abspath(__file__))
CLIENT_DIR = os.path.join(os.path.dirname(SERVER_DIR), 'client')
DATA_DIR = os.path.join(SERVER_DIR, 'data')

# Create necessary directories
os.makedirs(os.path.join(DATA_DIR, 'coupon'), exist_ok=True)
os.makedirs(os.path.join(DATA_DIR, 'market'), exist_ok=True)

def explain_sql(cursor, query, params=()):
    """Helper function to explain query plans"""
    cursor.execute(f"EXPLAIN QUERY PLAN {query}", params)
    return cursor.fetchall()

@app.route('/')
def root():
    return send_from_directory('../client', 'index.html')

@app.route('/<path:path>')
def serve_static(path):
    return send_from_directory('../client', path)

@app.route('/api/data/coupon')
def list_coupon_files():
    files = []
    coupon_dir = os.path.join(DATA_DIR, 'coupon')
    try:
        for f in os.listdir(coupon_dir):
            if f.endswith('.csv'):
                file_path = os.path.join(coupon_dir, f)
                files.append({
                    'name': f,
                    'size': os.path.getsize(file_path)
                })
    except Exception as e:
        logger.error(f"Error listing coupon files: {str(e)}")
    return jsonify(files)

@app.route('/api/data/market')
def list_market_files():
    files = []
    market_dir = os.path.join(DATA_DIR, 'market')
    try:
        for f in os.listdir(market_dir):
            if f.endswith('.csv') or f.endswith('.txt'):
                file_path = os.path.join(market_dir, f)
                files.append({
                    'name': f,
                    'size': os.path.getsize(file_path)
                })
    except Exception as e:
        logger.error(f"Error listing market files: {str(e)}")
    return jsonify(files)

@app.route('/api/data/coupon/<path:filename>')
def serve_coupon_data(filename):
    return send_from_directory(os.path.join(DATA_DIR, 'coupon'), filename)

@app.route('/api/data/market/<path:filename>')
def serve_market_data(filename):
    return send_from_directory(os.path.join(DATA_DIR, 'market'), filename)

@app.route('/api/flights/test')
def test_flights_db():
    try:
        db_path = os.path.join(SERVER_DIR, 'flights.db')
        logger.info(f"Checking database at: {db_path}")
        
        if not os.path.exists(db_path):
            return jsonify({'error': 'Database file not found'}), 404
            
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='flights'")
            if not cursor.fetchone():
                return jsonify({'error': 'Flights table not found'}), 404
            
            cursor.execute("SELECT COUNT(*) FROM flights")
            count = cursor.fetchone()[0]
            
            return jsonify({
                'status': 'success',
                'db_path': db_path,
                'count': count
            })
            
    except Exception as e:
        logger.error(f"Database test error: {str(e)}")
        return jsonify({'error': str(e)}), 500



@app.route('/api/flights/stream')
def stream_flights():
    try:
        logger.info("Starting to stream flights...")
        db_path = os.path.join(SERVER_DIR, 'flights.db')
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            
            query = """
                WITH sample_itineraries AS (
                    SELECT DISTINCT ItinID 
                    FROM flights 
                    ORDER BY year DESC, quarter DESC
                    LIMIT 100
                )
                SELECT f.year, f.quarter, f.ItinID, f.SeqNum, f.Coupons,
                       f.Origin, f.Dest, f.CouponType, f.TkCarrier, 
                       f.OpCarrier, f.RPCarrier, f.Passengers
                FROM flights f
                JOIN sample_itineraries si ON f.ItinID = si.ItinID
                ORDER BY f.ItinID, f.SeqNum
            """
            
            # Show query plan
            plan = explain_sql(cursor, query)
            logger.info(f"Query plan: {plan}")
            
            cursor.execute(query)
            columns = [description[0] for description in cursor.description]
            
            # Group results by ItinID
            results = {}
            for row in cursor.fetchall():
                row_dict = dict(zip(columns, row))
                itin_id = row_dict['ItinID']
                if itin_id not in results:
                    results[itin_id] = []
                results[itin_id].append(row_dict)
            
            logger.info(f"Sending {len(results)} itineraries...")
            return jsonify(results)
            
    except Exception as e:
        logger.error(f"Error streaming flights: {str(e)}")
        return jsonify({'error': str(e)}), 500





@app.route('/api/flights/stream/<itin_id>')
def stream_flights_by_itin(itin_id):
    try:
        logger.info(f"Looking up ItinID: {itin_id}")
        db_path = os.path.join(SERVER_DIR, 'flights.db')
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            
            # Create index for ItinID if it doesn't exist
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_itinid 
                ON flights(ItinID, year, quarter, SeqNum)
            """)
            
            query = """
                SELECT year, quarter, ItinID, SeqNum, Coupons,
                       Origin, Dest, CouponType, TkCarrier, 
                       OpCarrier, RPCarrier, Passengers
                FROM flights 
                INDEXED BY idx_itinid
                WHERE ItinID = ?
                ORDER BY year, quarter, SeqNum
            """
            
            # Show query plan
            plan = explain_sql(cursor, query, (itin_id,))
            logger.info(f"Query plan: {plan}")
            
            cursor.execute(query, (itin_id,))
            columns = [description[0] for description in cursor.description]
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            logger.info(f"Found {len(results)} segments for ItinID {itin_id}")
            return jsonify(results)
            
    except Exception as e:
        logger.error(f"Error looking up ItinID {itin_id}: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/flights/explain')
def explain_query_plans():
    try:
        db_path = os.path.join(SERVER_DIR, 'flights.db')
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            
            cursor.execute("EXPLAIN QUERY PLAN SELECT COUNT(*) FROM flights")
            count_plan = cursor.fetchall()
            
            cursor.execute("""
                EXPLAIN QUERY PLAN 
                SELECT MIN(year), MAX(year)
                FROM flights
            """)
            range_plan = cursor.fetchall()
            
            return jsonify({
                'count_plan': count_plan,
                'range_plan': range_plan
            })
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    logger.info(f"Server directory: {SERVER_DIR}")
    logger.info(f"Client directory: {CLIENT_DIR}")
    logger.info(f"Data directory: {DATA_DIR}")
    app.run(debug=True, port=5000)