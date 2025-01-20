import finnhub
import logging
import os
from typing import Dict, Optional
import time
from dotenv import load_dotenv
from file_utils import get_latest_mapping_file
import json

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class CompanyInfoClient:
    def __init__(self, api_key: str):
        self.client = finnhub.Client(api_key=api_key)
        self.last_call_time = 0
        self.calls_this_minute = 0
        self.rate_limit = 45  # calls per minute
        
    def _wait_for_rate_limit(self):
        """Ensure we don't exceed rate limits"""
        current_time = time.time()
        minute_ago = current_time - 60
        
        if self.last_call_time < minute_ago:
            # Reset if a minute has passed
            self.calls_this_minute = 0
        elif self.calls_this_minute >= self.rate_limit:
            # Wait until the minute is up
            sleep_time = 60 - (current_time - self.last_call_time)
            logging.info(f"Rate limit reached. Sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
            self.calls_this_minute = 0
            
        self.last_call_time = current_time
        self.calls_this_minute += 1

    def get_company_profile(self, symbol: str) -> Optional[Dict]:
        """Get company profile data from Finnhub"""
        self._wait_for_rate_limit()
        
        try:
            profile = self.client.company_profile2(symbol=symbol)
            if profile:
                logging.info(f"Found profile for {symbol}: {profile.get('name')}")
                return profile
            else:
                logging.warning(f"No profile found for {symbol}")
                return None
                
        except Exception as e:
            logging.error(f"Error getting profile for {symbol}: {str(e)}")
            return None

def fetch_all_company_profiles():
    """Fetch profiles for all companies in the latest mapping file"""
    # Check for API key
    api_key = os.getenv("FINNHUB_API_KEY")
    if not api_key:
        raise ValueError("FINNHUB_API_KEY environment variable not set")
    
    client = CompanyInfoClient(api_key)
    
    try:
        # Get latest mappings
        latest_file, ticker_data = get_latest_mapping_file()
        if not latest_file:
            logging.error("No mapping file found")
            return
            
        # Get all unique symbols
        symbols = set()
        for mapping in ticker_data["mappings"].values():
            if isinstance(mapping, dict):
                if mapping.get("sym"):
                    symbols.add(mapping["sym"])
            elif mapping:
                symbols.add(mapping)
        
        logging.info(f"Found {len(symbols)} unique symbols to process")
        
        # Fetch profiles
        profiles = {}
        for i, symbol in enumerate(symbols, 1):
            logging.info(f"Processing {symbol} ({i}/{len(symbols)})")
            profile = client.get_company_profile(symbol)
            if profile:
                profiles[symbol] = profile
        
        # Save results
        output_file = "company_profiles.json"
        with open(output_file, "w") as f:
            json.dump({
                "metadata": {
                    "total_symbols": len(symbols),
                    "profiles_found": len(profiles),
                    "source_file": latest_file
                },
                "profiles": profiles
            }, f, indent=2)
            
        logging.info(f"Saved {len(profiles)} profiles to {output_file}")
        
    except Exception as e:
        logging.error(f"Error fetching company profiles: {str(e)}")
        raise

def format_market_cap(market_cap: float) -> str:
    """Convert market cap from millions to billions/trillions with proper formatting"""
    if market_cap >= 1000000:  # Trillion
        return f"${market_cap/1000000:.2f}T"
    elif market_cap >= 1000:  # Billion
        return f"${market_cap/1000:.2f}B"
    else:  # Million
        return f"${market_cap:.2f}M"

def test_profile_lookup():
    """Test function to verify API calls are working"""
    api_key = os.getenv("FINNHUB_API_KEY")
    if not api_key:
        raise ValueError("FINNHUB_API_KEY environment variable not set")
    
    client = CompanyInfoClient(api_key)
    
    # Test with some well-known symbols
    test_symbols = ["AAPL", "MSFT", "GOOGL"]
    
    for symbol in test_symbols:
        logging.info(f"\nTesting lookup for {symbol}:")
        profile = client.get_company_profile(symbol)
        if profile:
            logging.info(f"Success! Found profile for {profile['name']}:")
            # Format market cap specially
            market_cap = profile.get('marketCapitalization')
            if market_cap:
                logging.info(f"  Market Cap: {format_market_cap(market_cap)}")
            
            # Log other interesting fields
            interesting_fields = [
                'exchange', 'finnhubIndustry', 'currency', 'weburl'
            ]
            for field in interesting_fields:
                if field in profile:
                    logging.info(f"  {field}: {profile[field]}")
        else:
            logging.error(f"No profile found for {symbol}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        test_profile_lookup()
    else:
        fetch_all_company_profiles() 