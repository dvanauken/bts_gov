# app/server/server.py
from flask import Flask, send_from_directory, jsonify
import os

app = Flask(__name__)

# Get the absolute path to the server directory
SERVER_DIR = os.path.dirname(os.path.abspath(__file__))
CLIENT_DIR = os.path.join(os.path.dirname(SERVER_DIR), 'client')
DATA_DIR = os.path.join(SERVER_DIR, 'data')

# Create necessary directories if they don't exist
os.makedirs(os.path.join(DATA_DIR, 'coupon'), exist_ok=True)
os.makedirs(os.path.join(DATA_DIR, 'market'), exist_ok=True)

# Serve static files from client directory
@app.route('/')
def root():
    return send_from_directory('../client', 'index.html')

@app.route('/<path:path>')
def serve_static(path):
    return send_from_directory('../client', path)

# API endpoints
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
        print(f"Error listing coupon files: {str(e)}")
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
        print(f"Error listing market files: {str(e)}")
    return jsonify(files)

@app.route('/api/data/coupon/<path:filename>')
def serve_coupon_data(filename):
    return send_from_directory(os.path.join(DATA_DIR, 'coupon'), filename)

@app.route('/api/data/market/<path:filename>')
def serve_market_data(filename):
    return send_from_directory(os.path.join(DATA_DIR, 'market'), filename)

if __name__ == '__main__':
    print(f"Server directory: {SERVER_DIR}")
    print(f"Client directory: {CLIENT_DIR}")
    print(f"Data directory: {DATA_DIR}")
    app.run(debug=True, port=5000)