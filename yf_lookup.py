import json
import time
import logging
import yfinance as yf
from typing import Optional, Dict
from pathlib import Path
import os
from datetime import datetime
from dotenv import load_dotenv
from file_utils import get_latest_mapping_file

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class YFinanceClient:
    def __init__(self):
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

    def lookup_symbol(self, company_name: str) -> Optional[str]:
        """Look up a company's ticker symbol using yfinance"""
        self._wait_for_rate_limit()
        
        try:
            # Common ticker mappings for well-known companies
            COMPANY_TICKERS = {
                "apple": "AAPL",
                "microsoft": "MSFT",
                "alphabet": "GOOGL",
                "google": "GOOGL",
                "amazon": "AMZN",
                "meta platforms": "META",
                "facebook": "META",
                "tesla": "TSLA",
                "berkshire hathaway": "BRK-B",
                "jpmorgan chase": "JPM",
                "johnson & johnson": "JNJ",
                "walmart": "WMT",
                "procter & gamble": "PG",
                "exxonmobil": "XOM",
                "coca-cola": "KO",
                "pfizer": "PFE",
                "bank of america": "BAC",
                "home depot": "HD",
                "chevron": "CVX",
                "cisco": "CSCO",
                "pepsico": "PEP",
                "adobe": "ADBE",
                "netflix": "NFLX",
                "costco": "COST",
                "thermo fisher": "TMO",
                "verizon": "VZ",
                "abbott": "ABT",
                "abbvie": "ABBV",
                "salesforce": "CRM",
                "merck": "MRK",
                "comcast": "CMCSA",
                "mcdonalds": "MCD",
                "t-mobile": "TMUS"
            }
            
            # Clean up company name
            name_to_search = company_name.lower()
            for suffix in [' inc.', ' inc', ' corp.', ' corp', ' corporation', ' ltd.', ' ltd', 
                          ' limited', ' plc', ' s.a.', ' ag', ' co.', ',', '.', ' company']:
                name_to_search = name_to_search.replace(suffix, '')
            name_to_search = name_to_search.strip()
            
            # Check common mappings first
            for known_name, ticker in COMPANY_TICKERS.items():
                if known_name in name_to_search:
                    # Verify the ticker is valid
                    try:
                        ticker_obj = yf.Ticker(ticker)
                        info = ticker_obj.info
                        if info:
                            return ticker
                    except:
                        pass
            
            # Try some common variations of the company name
            possible_tickers = [
                name_to_search.replace(' ', ''),  # No spaces
                name_to_search.replace(' ', '-'),  # With hyphens
                name_to_search.split()[0],        # First word only
            ]
            
            for possible_ticker in possible_tickers:
                try:
                    ticker_obj = yf.Ticker(possible_ticker)
                    info = ticker_obj.info
                    if info and info.get('symbol'):
                        return info['symbol']
                except:
                    continue
            
            return None
            
        except Exception as e:
            logging.error(f"Error looking up symbol for {company_name}: {str(e)}")
            return None

def get_next_available_filename(base_filename: str) -> str:
    """Get the next available filename by appending a number if the file exists"""
    if not os.path.exists(base_filename):
        return base_filename
    
    # Split filename into name and extension
    name, ext = os.path.splitext(base_filename)
    counter = 2
    
    while True:
        new_filename = f"{name}_{counter}{ext}"
        if not os.path.exists(new_filename):
            return new_filename
        counter += 1

def save_mappings(mappings: Dict[str, str], metadata: Dict, base_filename: str = "company_ticker_map.json"):
    """Save mappings to a JSON file with automatic numbering"""
    output_file = get_next_available_filename(base_filename)
    
    with open(output_file, "w") as f:
        json.dump({
            "metadata": metadata,
            "mappings": mappings
        }, f, indent=2)
    
    return output_file

def create_company_ticker_map():
    """Process all companies to create ticker mappings"""
    client = YFinanceClient()
    
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
    client = YFinanceClient()
    
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
                "total_companies": ticker_data["metadata"]["total_companies"] if ticker_data else len(updated_mappings),
                "mapped_companies": len(updated_mappings)
            }
            output_file = save_mappings(updated_mappings, metadata)
            
        elapsed_time = time.time() - start_time
        logging.info(f"Completed processing in {elapsed_time:.2f} seconds")
        logging.info(f"Found {newly_mapped} new mappings")
        logging.info(f"Total mapped companies now: {len(existing_mappings) + newly_mapped}")
        if latest_file:
            logging.info(f"Updated from {latest_file}")
        
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        raise

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--unmapped":
        process_unmapped_companies()
    else:
        create_company_ticker_map() 