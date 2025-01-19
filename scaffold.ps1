# Create base directory
New-Item -ItemType Directory -Path "bts_gov" -Force

# Create client directory and files
New-Item -ItemType Directory -Path "bts_gov/client" -Force
New-Item -ItemType File -Path "bts_gov/client/index.html" -Force
New-Item -ItemType File -Path "bts_gov/client/style.css" -Force
New-Item -ItemType File -Path "bts_gov/client/script.js" -Force

# Create server directory and files
New-Item -ItemType Directory -Path "bts_gov/server" -Force
New-Item -ItemType File -Path "bts_gov/server/requirements.txt" -Force
New-Item -ItemType File -Path "bts_gov/server/config.yml" -Force
New-Item -ItemType File -Path "bts_gov/server/config_reader.py" -Force
New-Item -ItemType File -Path "bts_gov/server/db1b_coupon.py" -Force
New-Item -ItemType File -Path "bts_gov/server/db1b_market.py" -Force

# Create data directories
New-Item -ItemType Directory -Path "bts_gov/server/data" -Force
New-Item -ItemType Directory -Path "bts_gov/server/data/coupon" -Force
New-Item -ItemType Directory -Path "bts_gov/server/data/market" -Force

# Show the created structure
Get-ChildItem -Path "bts_gov" -Recurse