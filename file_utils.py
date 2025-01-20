import os
import json
import glob
from typing import Dict, Tuple, Optional

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

def get_latest_mapping_file() -> Tuple[Optional[str], Optional[Dict]]:
    """
    Find the latest company_ticker_map file and load its contents.
    Returns tuple of (filename, contents) or (None, None) if no file found.
    """
    # Get all company_ticker_map files
    files = glob.glob("company_ticker_map*.json")
    
    if not files:
        return None, None
        
    # Sort files by modification time, newest first
    latest_file = max(files, key=os.path.getmtime)
    
    try:
        with open(latest_file, 'r') as f:
            contents = json.load(f)
        return latest_file, contents
    except Exception as e:
        print(f"Error reading {latest_file}: {str(e)}")
        return None, None

def get_total_companies() -> int:
    """Get total number of companies from enriched_market_matrix.json"""
    try:
        with open("enriched_market_matrix.json", "r") as f:
            market_data = json.load(f)
            
        total = 0
        for sector in market_data["sectors"]:
            for subsector in sector["subsectors"]:
                total += len(subsector["companies"])
        return total
    except Exception as e:
        print(f"Error reading market matrix: {str(e)}")
        return 0

def save_mappings(mappings: Dict[str, str], metadata: Dict, base_filename: str = "company_ticker_map.json"):
    """Save mappings to a JSON file with automatic numbering"""
    output_file = get_next_available_filename(base_filename)
    
    # Get actual total companies count
    total_companies = get_total_companies()
    
    # Update metadata with correct total
    metadata["total_companies"] = total_companies
    
    with open(output_file, "w") as f:
        json.dump({
            "metadata": metadata,
            "mappings": mappings
        }, f, indent=2)
    
    return output_file 