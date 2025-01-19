import yaml
from typing import List, Dict, Any
from datetime import datetime
import os

def parse_range(items: List[str]) -> List[int]:
    """Parse a list that may contain ranges (e.g., ['2012...2015', '2018'])"""
    result = set()
    for item in items:
        if isinstance(item, int):
            result.add(item)
        elif isinstance(item, str) and '...' in item:
            start, end = map(int, item.split('...'))
            result.update(range(start, end + 1))
        else:
            result.add(int(item))
    return sorted(list(result))

class ConfigReader:
    def __init__(self, config_file: str = 'config.yml'):
        # Get the directory where the script is located
        script_dir = os.path.dirname(os.path.abspath(__file__))
        # Create absolute path to config file
        config_path = os.path.join(script_dir, config_file)
        
        print(f"Looking for config file at: {config_path}")  # Debug message
        
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

    @property
    def download_config(self) -> Dict[str, Any]:
        return self.config['download']

    @property
    def db1b_config(self) -> Dict[str, Any]:
        return self.config.get('db1b_coupon', self.config['db1b_market'])

    @property
    def years(self) -> List[int]:
        return parse_range(self.db1b_config['years'])

    @property
    def quarters(self) -> List[int]:
        return parse_range(self.db1b_config['quarters'])

    @property
    def base_url(self) -> str:
        return self.db1b_config['base_url']

    def get_download_pairs(self) -> List[tuple]:
        """Get all year-quarter pairs to download"""
        pairs = []
        for year in self.years:
            for quarter in self.quarters:
                pairs.append((year, quarter))
        return pairs

if __name__ == "__main__":
    # Test the config reader
    config = ConfigReader()
    print("Years:", config.years)
    print("Quarters:", config.quarters)
    print("Download pairs:", config.get_download_pairs())