# BTS Government Data Processing Server

This project provides a server for processing Bureau of Transportation Statistics (BTS) DB1B (Airline Origin and Destination Survey) data.

## Prerequisites

- Python 3.8 or higher
- pip (Python package installer)

## Installation

1. Clone the repository:
```bash
git clone [your-repository-url]
cd bts_gov
```

2. Install the required dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

1. Create or modify the `app/server/config.yml` file with your desired settings:
```yaml
download:
  verify_ssl: false
  max_concurrent: 3

db1b_coupon:
  base_url: "https://example.com/data/db1b_coupon"
  years: ["2023"]
  quarters: ["1", "2", "3", "4"]

db1b_market:
  base_url: "https://example.com/data/db1b_market"
  years: ["2023"]
  quarters: ["1", "2", "3", "4"]
```

## Project Structure

```
├── app
│   ├── client
│   │   ├── index.html
│   │   ├── script.js
│   │   └── style.css
│   └── server
│       ├── config.yml
│       ├── config_reader.py
│       ├── data
│       ├── db1b_coupon.py
│       ├── db1b_market.py
│       └── server.py
```

## Running the Server

Before starting the server, ensure you have the correct directory structure:

1. Create the required data directories:
```bash
mkdir -p data/coupon
mkdir -p data/market
```

2. Make sure you have a client directory with the required files:
```bash
app/
└── client/
    ├── index.html
    ├── script.js
    └── style.css
```

3. Start the Flask server:
```bash
# From the project root directory (bts_gov)
python -m flask --app app.server.server run --debug
```

or alternatively:

```bash
# From the project root directory (bts_gov)
cd app/server
python server.py
```

The server will start on `http://localhost:5000`

You should see output similar to:
```
 * Serving Flask app 'server'
 * Debug mode: on
 * Running on http://127.0.0.1:5000
```

### Troubleshooting Server Startup

1. If you get a "No module named 'flask'" error:
   ```bash
   pip install flask
   ```

2. If you get a "No such file or directory" error for data/coupon or data/market:
   - Make sure you've created the data directories as shown in step 1

3. If you get a "No such file or directory" error for client files:
   - Ensure your client directory exists in app/client
   - Make sure index.html exists in the client directory

4. If the server starts but you can't access the webpage:
   - Check that you're using the correct URL (http://localhost:5000)
   - Verify that all client files (index.html, script.js, style.css) exist
   - Check the Flask server console for any error messages

## Data Processing Scripts

### Processing Coupon Data

To process DB1B Coupon data:
```bash
python app/server/db1b_coupon.py
```

### Processing Market Data

To process DB1B Market data:
```bash
python app/server/db1b_market.py
```

## API Endpoints

The server provides the following API endpoints:

- `GET /`: Serves the main application page
- `GET /api/data/coupon`: Lists all coupon data files
- `GET /api/data/market`: Lists all market data files
- `GET /api/data/coupon/<filename>`: Serves a specific coupon data file
- `GET /api/data/market/<filename>`: Serves a specific market data file

## Data Storage

- Coupon data is stored in `data/coupon/`
- Market data is stored in `data/market/`

The directories will be created automatically when running the processing scripts.

## File Naming Convention

Processed files follow this naming pattern:
- Coupon: `DB1B_COUPON_SLIM_[YEAR]_[QUARTER].[TIMESTAMP].csv`
- Market: `CITY_PAIR_[YEAR]_[QUARTER].[TIMESTAMP].txt`

## Error Handling

- The scripts include graceful handling of keyboard interrupts (Ctrl+C)
- Progress bars show download and processing status
- Detailed error messages are displayed if issues occur

## Debugging

If you encounter issues:
1. Check the config.yml file exists and is properly formatted
2. Ensure all required directories exist
3. Verify your Python environment has all required dependencies
4. Check the server logs for detailed error messages

## Contributing

[Add your contribution guidelines here]

## License

[Add your license information here]