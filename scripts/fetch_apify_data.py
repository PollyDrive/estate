#!/usr/bin/env python3
"""
Fetch data from Apify run to debug ID extraction.
"""

import os
import sys
import json
from pathlib import Path
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from apify_client import ApifyClient

def main():
    load_dotenv()
    
    api_key = os.getenv('APIFY_API_KEY')
    if not api_key:
        print("Error: APIFY_API_KEY not set in environment")
        sys.exit(1)
    
    run_id = '9s566IqoHEshpEbbS'
    
    client = ApifyClient(api_key)
    
    try:
        # Get run info
        run = client.run(run_id).get()
        dataset_id = run['defaultDatasetId']
        
        print(f"Run ID: {run_id}")
        print(f"Dataset ID: {dataset_id}")
        print(f"Status: {run['status']}")
        print()
        
        # Get items
        items = list(client.dataset(dataset_id).iterate_items())
        
        print(f"Total items: {len(items)}")
        print("=" * 80)
        
        for i, item in enumerate(items):
            print(f"\n[Item {i+1}]")
            print(f"Keys: {list(item.keys())}")
            
            if 'moreDetails' in item:
                more_details = item['moreDetails']
                print(f"moreDetails keys: {list(more_details.keys())}")
                
                # Check for ID fields
                print(f"\nID fields:")
                print(f"  raw.id: {item.get('id')}")
                print(f"  raw.listing_id: {item.get('listing_id')}")
                print(f"  moreDetails.id: {more_details.get('id')}")
                print(f"  moreDetails.listing_id: {more_details.get('listing_id')}")
                
                # Check title
                title = more_details.get('marketplace_listing_title') or more_details.get('base_marketplace_listing_title')
                print(f"\nTitle: {title}")
                
                # Check if there's a URL we can parse
                print(f"\nURL fields:")
                print(f"  raw.url: {item.get('url')}")
                print(f"  raw.listingUrl: {item.get('listingUrl')}")
                print(f"  moreDetails.url: {more_details.get('url')}")
                
            print("-" * 80)
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()
