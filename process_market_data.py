import json
import asyncio
import os
from typing import Dict, List
import time
from openai import OpenAI
from dotenv import load_dotenv
import logging
from file_utils import get_latest_mapping_file

load_dotenv()
client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))

async def get_companies_for_subsector(sector: str, subsector: str) -> Dict:
    """
    Make an OpenAI API call to get top companies for a specific subsector.
    """
    print(f"Processing {sector} - {subsector}...")
    
    system_prompt = """
    You are a financial market expert specialized in providing accurate company data. 
    Your task is to identify the top 9 companies by market capitalization for a specific industry subsector.
    
    Guidelines:
    1. Only include publicly traded companies
    2. Ensure companies are primarily focused in the specified subsector
    3. Provide current market capitalization in billions/trillions (B/T)
    4. Sort companies by market cap (highest to lowest)
    5. Only include real, verifiable companies
    """

    try:
        response = client.chat.completions.create(
            model="gpt-4o-2024-08-06",
            messages=[
                {
                    "role": "system",
                    "content": system_prompt
                },
                {
                    "role": "user",
                    "content": f"Please provide the top 9 companies in the {sector} sector, specifically in the {subsector} subsector."
                }
            ],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "get_companies_for_subsector",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "companies": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "name": {
                                            "type": "string",
                                            "description": "Company name"
                                        },
                                        "market_cap": {
                                            "type": "string",
                                            "description": "Market capitalization with B/T suffix (e.g., '2.5T' or '500B')"
                                        }
                                    },
                                    "required": ["name", "market_cap"]
                                },
                                "minItems": 9,
                                "maxItems": 9
                            }
                        },
                        "required": ["companies"],
                        "additionalProperties": False
                    }
                }
            }
        )
        
        result = json.loads(response.choices[0].message.content)
        print(f"✓ Completed {sector} - {subsector}")
        return result
    except Exception as e:
        print(f"✗ Error processing {sector} - {subsector}: {str(e)}")
        return {"companies": [{"name": f"Error processing {subsector}", "market_cap": "N/A"}] * 9}

async def process_all_sectors(input_file: str, output_file: str):
    """
    Main function to process all sectors and subsectors in parallel.
    """
    try:
        print("\nStarting market data processing...")
        print("Reading input file...")
        
        # Read input JSON
        with open(input_file, 'r') as f:
            market_data = json.load(f)
        
        print(f"\nProcessing {len(market_data['sectors'])} sectors with 3 subsectors each...")
        print("This will take several minutes. Processing 5 subsectors at a time...\n")
        
        # Process all subsectors in parallel with a semaphore to limit concurrent API calls
        semaphore = asyncio.Semaphore(5)  # Limit to 5 concurrent API calls
        
        async def bounded_get_companies(sector_name: str, subsector: str) -> Dict:
            async with semaphore:
                return await get_companies_for_subsector(sector_name, subsector)
        
        # Create bounded tasks
        tasks = [
            bounded_get_companies(sector["name"], subsector)
            for sector in market_data["sectors"]
            for subsector in sector["subsectors"]
        ]
        
        # Process all subsectors
        results = await asyncio.gather(*tasks)
        
        print("\nUpdating market data with results...")
        
        # Update the market data with results
        result_index = 0
        for sector in market_data["sectors"]:
            enriched_subsectors = []
            for subsector in sector["subsectors"]:
                enriched_subsectors.append({
                    "name": subsector,
                    "companies": results[result_index]["companies"]
                })
                result_index += 1
            sector["subsectors"] = enriched_subsectors
        
        # Save enriched data to new JSON file
        print(f"\nSaving results to {output_file}...")
        with open(output_file, 'w') as f:
            json.dump(market_data, f, indent=2)
        
        print(f"\nSuccessfully processed {len(results)} subsectors")
        return market_data

    except Exception as e:
        print(f"\nError processing market data: {str(e)}")
        raise

def main():
    """
    Entry point of the script.
    """
    start_time = time.time()
    
    # Run the async function
    asyncio.run(process_all_sectors(
        input_file='market_matrix.json',
        output_file='enriched_market_matrix.json'
    ))
    
    end_time = time.time()
    print(f"\nTotal processing time: {end_time - start_time:.2f} seconds")

def clean_json_response(response_text: str) -> str:
    """Clean response text to get valid JSON"""
    # logging.debug(f"Original response: {repr(response_text)}")
    
    # Remove markdown code blocks if present
    if "```" in response_text:
        # Split and clean
        parts = response_text.split("```")
        # logging.debug(f"Split parts: {repr(parts)}")
        
        # Find the part that contains the JSON
        for part in parts:
            cleaned = part.strip()
            if cleaned:
                # Remove "json" prefix if present
                if cleaned.lower().startswith('json\n'):
                    cleaned = cleaned[5:]  # Remove "json\n"
                cleaned = cleaned.strip()
                
                # logging.debug(f"Cleaned part: {repr(cleaned)}")
                # Try to parse it to validate
                try:
                    json.loads(cleaned)
                    logging.debug("Successfully validated JSON")
                    return cleaned
                except json.JSONDecodeError:
                    logging.debug(f"Failed to validate part as JSON: {repr(cleaned)}")
                    continue
    
    # If we get here, try the original response
    try:
        json.loads(response_text.strip())
        return response_text.strip()
    except json.JSONDecodeError:
        logging.error("Could not find valid JSON in response")
        return response_text.strip()

def clean_company_name(name: str) -> str:
    """Remove parenthetical suffixes and clean company name"""
    # Remove anything in parentheses at the end of the name
    if '(' in name:
        name = name.split('(')[0]
    
    # Clean up any trailing spaces, commas, etc
    name = name.strip(' ,.;')
    
    return name

def get_tickers_from_perplexity(companies: List[Dict]) -> List[Dict]:
    """
    Get ticker symbols for companies using Perplexity AI.
    Returns list of {name, sym, notes} objects.
    """
    api_key = os.getenv("PPL_API_KEY")
    if not api_key:
        raise ValueError("PPL_API_KEY environment variable not set")
    
    client = OpenAI(api_key=api_key, base_url="https://api.perplexity.ai")
    
    # Clean company names and create the list
    cleaned_companies = [
        {
            "name": clean_company_name(company['name']),
            "original_name": company['name'],
            "market_cap": company['market_cap']
        }
        for company in companies
    ]
    
    # Create the prompt with cleaned names
    company_list = "\n".join([
        f"- {company['name']} (Market Cap: {company['market_cap']})" 
        for company in cleaned_companies
    ])
    
    messages = [
        {
            "role": "system",
            "content": (
                "You are a financial data expert providing stock ticker symbols in JSON format. "
                "Rules:\n"
                "1. Return a JSON array of objects with keys: 'name', 'sym', 'notes'\n"
                "2. For major public companies, always provide the primary exchange ticker\n"
                "3. Set 'notes' to null for successfully found tickers\n"
                "4. Only set 'sym' to null if absolutely no ticker exists\n"
                "5. For special cases, provide details in 'notes':\n"
                "   - ADR tickers (note primary listing)\n"
                "   - Multiple listings (list alternatives)\n"
                "   - Private companies (explain status)\n"
                "6. Use primary US exchange ticker when available (NYSE/NASDAQ)\n"
                "7. No text outside the JSON array\n"
                "8. No markdown formatting\n\n"
                "Example: [{\"name\": \"Apple Inc.\", \"sym\": \"AAPL\", \"notes\": null}]"
            ),
        },
        {
            "role": "user",
            "content": f"Return ticker symbols for:\n{company_list}"
        },
    ]
    
    try:
        response = client.chat.completions.create(
            model="llama-3.1-sonar-large-128k-online",
            messages=messages,
        )
        
        # Parse the response and map back to original names
        response_text = response.choices[0].message.content
        json_str = clean_json_response(response_text)
        
        try:
            result = json.loads(json_str)
            if isinstance(result, list):
                # Map results back to original names
                final_results = []
                name_map = {clean_company_name(c['name']): c['original_name'] for c in cleaned_companies}
                
                for item in result:
                    original_name = name_map.get(item['name'], item['name'])
                    final_results.append({
                        "name": original_name,
                        "sym": item['sym'],
                        "notes": item['notes']
                    })
                    
                    if item.get('notes'):
                        logging.info(f"{original_name} -> {item['sym']} ({item['notes']})")
                    else:
                        logging.info(f"{original_name} -> {item['sym']}")
                        
                return final_results
            else:
                logging.error(f"Unexpected response format: {result}")
                return []
                
        except json.JSONDecodeError as e:
            logging.error(f"Failed to parse JSON response: {str(e)}")
            logging.error(f"Raw response: {repr(response_text)}")
            return []
        
    except Exception as e:
        logging.error(f"Error getting tickers from Perplexity: {str(e)}")
        return []

def process_unmapped_with_perplexity(batch_size: int = 20):
    """
    Process unmapped companies using Perplexity AI to find ticker symbols.
    Processes companies in batches to avoid hitting API limits.
    """
    try:
        # Read unmapped companies
        with open("unmapped_companies.json", "r") as f:
            unmapped_data = json.load(f)
        
        # Get latest mappings
        _, ticker_data = get_latest_mapping_file()
        existing_mappings = ticker_data["mappings"] if ticker_data else {}
        
        # Process in batches
        companies = unmapped_data["unmapped"]
        total_companies = len(companies)
        new_mappings = {}
        
        for i in range(0, total_companies, batch_size):
            batch = companies[i:i + batch_size]
            logging.info(f"Processing batch {i//batch_size + 1} ({i+1}-{min(i+batch_size, total_companies)}/{total_companies})")
            
            results = get_tickers_from_perplexity(batch)
            
            # Add new mappings
            for result in results:
                new_mappings[result["name"]] = result["sym"]
        
        # Save results
        if new_mappings:
            updated_mappings = {**existing_mappings, **new_mappings}
            metadata = {
                "source": "perplexity_ai",
                "total_companies": unmapped_data["metadata"]["total_companies"],
                "mapped_companies": len(updated_mappings)
            }
            
            from file_utils import save_mappings
            output_file = save_mappings(updated_mappings, metadata)
            logging.info(f"Saved {len(new_mappings)} new mappings to {output_file}")
        else:
            logging.info("No new mappings found")
            
    except Exception as e:
        logging.error(f"Error processing unmapped companies: {str(e)}")
        raise

def test_perplexity_lookup():
    """
    Test function to verify Perplexity API calls are working correctly.
    Processes just one small batch of well-known companies.
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Test with a small batch of well-known companies
    test_companies = [
        {"name": "Apple Inc.", "market_cap": "3T"},
        {"name": "Microsoft Corporation", "market_cap": "2.8T"},
        {"name": "Alphabet Inc.", "market_cap": "1.8T"},
        {"name": "Amazon", "market_cap": "1.6T"},
        {"name": "NVIDIA Corporation", "market_cap": "1.5T"}
    ]
    
    try:
        logging.info("Testing Perplexity API with sample companies...")
        results = get_tickers_from_perplexity(test_companies)
        
        if results:
            logging.info("Success! Got the following results:")
            for company in results:
                logging.info(f"{company['name']} -> {company['sym']}")
        else:
            logging.error("No results returned from API")
            
    except Exception as e:
        logging.error(f"Test failed: {str(e)}")

if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.DEBUG,  # Set to DEBUG to see all logging
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    # test_perplexity_lookup()
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        test_perplexity_lookup()
    else:
        process_unmapped_with_perplexity() 