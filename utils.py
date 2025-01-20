import json
import logging
from typing import Set, Dict, Any
from pathlib import Path
from file_utils import get_latest_mapping_file

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def find_unmapped_companies() -> Set[str]:
    """
    Compare enriched_market_matrix.json with latest company_ticker_map file
    to find companies that don't have ticker mappings.
    
    Returns:
        Set of company names that don't have ticker mappings
    """
    try:
        # Read the market matrix
        with open("enriched_market_matrix.json", "r") as f:
            market_data = json.load(f)
            
        # Read the latest ticker mappings
        latest_file, ticker_data = get_latest_mapping_file()
        if latest_file:
            logging.info(f"Using mappings from {latest_file}")
            # Get ALL companies from mapping file, regardless of ticker value
            mapped_companies = set(ticker_data["mappings"].keys())
            logging.info(f"Found {len(mapped_companies)} mapped companies")
        else:
            logging.warning("No existing mapping file found")
            mapped_companies = set()
        
        # Get all companies from market matrix
        all_companies = set()
        for sector in market_data["sectors"]:
            for subsector in sector["subsectors"]:
                for company in subsector["companies"]:
                    all_companies.add(company["name"])
        
        logging.info(f"Total companies in market matrix: {len(all_companies)}")
        
        # Find unmapped companies (those in market matrix but not in mapping file)
        unmapped_companies = all_companies - mapped_companies
        
        # Log some sample unmapped companies for verification
        logging.info(f"\nSample of unmapped companies (up to 10):")
        for company in list(unmapped_companies)[:10]:
            logging.info(f"  - {company}")
            
        # Log some sample mapped companies for verification
        logging.info(f"\nSample of mapped companies (up to 10):")
        for company in list(mapped_companies)[:10]:
            logging.info(f"  - {company}")
        
        # Create structured data with sector/subsector info for unmapped companies
        unmapped_details = []
        for sector in market_data["sectors"]:
            for subsector in sector["subsectors"]:
                for company in subsector["companies"]:
                    if company["name"] in unmapped_companies:
                        unmapped_details.append({
                            "name": company["name"],
                            "sector": sector["name"],
                            "subsector": subsector["name"],
                            "market_cap": company["market_cap"]
                        })
        
        # Sort by market cap (removing B, T, M and converting to float)
        def market_cap_to_float(cap: str) -> float:
            if cap in ["N/A", "Not publicly traded"]:
                return 0.0
            try:
                multiplier = {"T": 1e12, "B": 1e9, "M": 1e6}
                number = float(cap[:-1])
                unit = cap[-1]
                return number * multiplier.get(unit, 1)  # Default multiplier of 1 if unit not found
            except (ValueError, IndexError):
                logging.warning(f"Could not parse market cap: {cap}")
                return 0.0
        
        unmapped_details.sort(
            key=lambda x: market_cap_to_float(x["market_cap"]),
            reverse=True
        )
        
        # Save to file
        output_file = "unmapped_companies.json"
        with open(output_file, "w") as f:
            json.dump({
                "metadata": {
                    "total_companies": len(all_companies),
                    "mapped_companies": len(mapped_companies),
                    "unmapped_companies": len(unmapped_companies),
                    "latest_mapping_file": latest_file
                },
                "unmapped": unmapped_details
            }, f, indent=2)
            
        logging.info(f"Found {len(unmapped_companies)} unmapped companies")
        logging.info(f"Results saved to {output_file}")
        
        return unmapped_companies
        
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        raise

def count_unique_tickers() -> int:
    """
    Get count of unique ticker symbols from the latest company_ticker_map file.
    Excludes null values and logs the count.
    """
    try:
        # Get latest mapping file
        latest_file, ticker_data = get_latest_mapping_file()
        if not ticker_data:
            logging.error("No mapping file found")
            return 0
            
        # Get all non-null ticker symbols, handling both old and new format
        ticker_symbols = set()
        for value in ticker_data["mappings"].values():
            if isinstance(value, dict):
                # New format with sym and notes
                if value.get("sym") is not None:
                    ticker_symbols.add(value["sym"])
            elif value is not None:
                # Old format with just ticker string
                ticker_symbols.add(value)
        
        # Log results
        logging.info(f"Found {len(ticker_symbols)} unique ticker symbols in {latest_file}")
        logging.info(f"Total mappings: {len(ticker_data['mappings'])}")
        
        return len(ticker_symbols)
        
    except Exception as e:
        logging.error(f"Error counting unique tickers: {str(e)}")
        return 0

def clean_duplicate_tickers():
    """
    Clean up the latest company ticker map by identifying and resolving duplicate ticker mappings.
    Creates a new mapping file with duplicates resolved.
    """
    try:
        # Get latest mapping file
        latest_file, ticker_data = get_latest_mapping_file()
        if not ticker_data:
            logging.error("No mapping file found")
            return
            
        # Get total companies from enriched market matrix
        with open("enriched_market_matrix.json", "r") as f:
            market_data = json.load(f)
            
        total_companies = sum(
            len(subsector["companies"])
            for sector in market_data["sectors"]
            for subsector in sector["subsectors"]
        )
            
        # Find companies mapping to the same ticker
        ticker_to_companies = {}
        for company, ticker in ticker_data["mappings"].items():
            if ticker is not None:  # Skip null tickers
                if ticker not in ticker_to_companies:
                    ticker_to_companies[ticker] = []
                ticker_to_companies[ticker].append(company)
        
        # Identify duplicates
        duplicates = {ticker: companies for ticker, companies in ticker_to_companies.items() 
                     if len(companies) > 1}
        
        if duplicates:
            logging.info(f"Found {len(duplicates)} tickers with multiple companies:")
            for ticker, companies in duplicates.items():
                logging.info(f"\nTicker {ticker} is used by:")
                for company in companies:
                    logging.info(f"  - {company}")
            
            # Create new mappings with notes about duplicates
            new_mappings = {}
            for company, ticker in ticker_data["mappings"].items():
                if ticker in duplicates:
                    # Add a note about the duplicate
                    new_mappings[company] = {
                        "sym": ticker,
                        "notes": f"Duplicate ticker used by: {', '.join(c for c in duplicates[ticker] if c != company)}"
                    }
                else:
                    new_mappings[company] = {
                        "sym": ticker,
                        "notes": None
                    }
            
            # Save new mapping file with corrected counts, without duplicate_tickers
            metadata = {
                "source": "duplicate_cleanup",
                "original_file": latest_file,
                "total_companies": total_companies,  # From enriched market matrix
                "mapped_companies": len(ticker_to_companies)  # Count of unique tickers
            }
            
            from file_utils import save_mappings
            output_file = save_mappings(new_mappings, metadata)
            logging.info(f"\nCreated new mapping file: {output_file}")
            logging.info(f"Total companies in market matrix: {total_companies}")
            logging.info(f"Original mappings: {len(ticker_data['mappings'])}")
            logging.info(f"Unique tickers: {len(ticker_to_companies)}")
            logging.info(f"Duplicate tickers found: {len(duplicates)}")  # Move to logging only
            
        else:
            logging.info("No duplicate ticker mappings found!")
            
    except Exception as e:
        logging.error(f"Error cleaning duplicate tickers: {str(e)}")
        raise

if __name__ == "__main__":
    find_unmapped_companies()
    # count_unique_tickers()
    # clean_duplicate_tickers() 