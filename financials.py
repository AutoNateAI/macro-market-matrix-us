import json
import time
import logging
import finnhub
from typing import Optional, Dict
from pathlib import Path
import os
from datetime import datetime
from dotenv import load_dotenv
from file_utils import save_mappings, get_latest_mapping_file

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class FinnhubClient:
    def __init__(self, api_key: str):
        self.client = finnhub.Client(api_key=api_key)
        self.last_call_time = 0
        self.calls_this_minute = 0
        self.rate_limit = 45  # calls per minute
        self.COMPANY_ALIASES = {
            "alphabet inc": "google",
            "meta platforms": "facebook",
            "amazon": "amazon.com",
            "berkshire hathaway": "brk",
        }
        
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

    def lookup_symbol(self, company_name: str) -> Optional[str]:
        """Look up a company's ticker symbol using Finnhub API"""
        self._wait_for_rate_limit()
        
        # Clean up company name and check aliases
        name_to_search = company_name.lower()
        for suffix in [' inc.', ' inc', ' corp.', ' corp', ' corporation', ' ltd.', ' ltd', 
                      ' limited', ' plc', ' s.a.', ' ag', ' co.', ',', '.']:
            name_to_search = name_to_search.replace(suffix, '')
        name_to_search = name_to_search.strip()
        
        # Check aliases
        for alias_key, alias_value in self.COMPANY_ALIASES.items():
            if alias_key in name_to_search:
                name_to_search = alias_value
                break
        
        try:
            results = self.client.symbol_lookup(name_to_search)
            
            if results.get('result'):
                logging.debug(f"Raw results for {company_name}: {results['result']}")
                
                # First try exact matches with US listings
                for result in results['result']:
                    if (result["type"] == "Common Stock" and 
                        '.' not in result["symbol"] and  # US listing
                        any(name in result["description"].lower() for name in [name_to_search, company_name.lower()])):
                        return result["symbol"]
                
                # Then try partial matches with US listings
                for result in results['result']:
                    if (result["type"] == "Common Stock" and 
                        '.' not in result["symbol"]):  # US listing
                        desc_words = set(result["description"].lower().split())
                        search_words = set(name_to_search.split())
                        # If more than 50% of the search words are in the description
                        if len(search_words & desc_words) / len(search_words) > 0.5:
                            return result["symbol"]
                
                # Finally, try any common stock
                for result in results['result']:
                    if result["type"] == "Common Stock":
                        return result["symbol"]
                
            return None
            
        except Exception as e:
            logging.error(f"Error looking up symbol for {company_name}: {str(e)}")
            return None

def create_company_ticker_map():
    """Process all companies to create ticker mappings"""
    # Check for API key
    api_key = os.getenv("FINNHUB_API_KEY")
    if not api_key:
        raise ValueError("FINNHUB_API_KEY environment variable not set")
    
    client = FinnhubClient(api_key)
    
    try:
        # Read the market matrix
        with open("enriched_market_matrix.json", "r") as f:
            market_data = json.load(f)
        
        company_ticker_map: Dict[str, str] = {}
        total_companies = 0
        processed_companies = 0
        
        # Count total companies
        for sector in market_data["sectors"]:
            for subsector in sector["subsectors"]:
                total_companies += len(subsector["companies"])
        
        # Process each company
        start_time = time.time()
        for sector in market_data["sectors"]:
            for subsector in sector["subsectors"]:
                for company in subsector["companies"]:
                    company_name = company["name"]
                    processed_companies += 1
                    
                    if company_name not in company_ticker_map:
                        logging.info(f"Processing {company_name} ({processed_companies}/{total_companies})")
                        ticker = client.lookup_symbol(company_name)
                        if ticker:
                            company_ticker_map[company_name] = ticker
                            logging.info(f"Found ticker for {company_name}: {ticker}")
                        else:
                            logging.warning(f"No ticker found for {company_name}")
        
        # Save the results
        metadata = {
            "created_at": datetime.now().isoformat(),
            "total_companies": total_companies,
            "mapped_companies": len(company_ticker_map)
        }
        output_file = save_mappings(company_ticker_map, metadata)
        
        elapsed_time = time.time() - start_time
        logging.info(f"Completed processing in {elapsed_time:.2f} seconds")
        logging.info(f"Results saved to {output_file}")
        logging.info(f"Successfully mapped {len(company_ticker_map)}/{total_companies} companies")
        
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        raise

def process_unmapped_companies():
    """Process only the unmapped companies to find their ticker symbols"""
    # Check for API key
    api_key = os.getenv("FINNHUB_API_KEY")
    if not api_key:
        raise ValueError("FINNHUB_API_KEY environment variable not set")
    
    client = FinnhubClient(api_key)
    
    try:
        # Read the unmapped companies
        with open("unmapped_companies.json", "r") as f:
            unmapped_data = json.load(f)
        
        # Read latest ticker mappings
        latest_file, ticker_data = get_latest_mapping_file()
        existing_mappings = ticker_data["mappings"] if ticker_data else {}
        
        new_mappings = {}
        total_unmapped = len(unmapped_data["unmapped"])
        processed = 0
        newly_mapped = 0
        
        # Process each unmapped company
        start_time = time.time()
        for company in unmapped_data["unmapped"]:
            company_name = company["name"]
            processed += 1
            
            if company_name not in existing_mappings:
                logging.info(f"Processing {company_name} ({processed}/{total_unmapped})")
                ticker = client.lookup_symbol(company_name)
                if ticker:
                    new_mappings[company_name] = ticker
                    newly_mapped += 1
                    logging.info(f"Found ticker for {company_name}: {ticker}")
                else:
                    logging.warning(f"No ticker found for {company_name}")
        
        # Update and save the mappings
        if new_mappings:
            updated_mappings = {**existing_mappings, **new_mappings}
            metadata = {
                "created_at": datetime.now().isoformat(),
                "total_companies": ticker_data["metadata"]["total_companies"],
                "mapped_companies": len(updated_mappings)
            }
            output_file = save_mappings(updated_mappings, metadata)
            
        elapsed_time = time.time() - start_time
        logging.info(f"Completed processing in {elapsed_time:.2f} seconds")
        logging.info(f"Found {newly_mapped} new mappings")
        logging.info(f"Total mapped companies now: {len(existing_mappings) + newly_mapped}")
        
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        raise

if __name__ == "__main__":
    # You can choose which function to run
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--unmapped":
        process_unmapped_companies()
    else:
        create_company_ticker_map() 