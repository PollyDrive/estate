#!/usr/bin/env python3
"""
Test to see what the memo23/facebook-marketplace-cheerio actor actually returns.
"""

import os
import json
import sys
from dotenv import load_dotenv
sys.path.insert(0, 'src')

from apify_client import ApifyClient

load_dotenv()

def test_actor_output():
    """Test what the actor returns for a single item URL."""
    
    api_key = os.getenv('APIFY_API_KEY')
    if not api_key:
        print("Error: APIFY_API_KEY not found")
        return
    
    client = ApifyClient(api_key)
    
    # Test URL from config
    test_url = "https://www.facebook.com/marketplace/item/1514733676311943"
    
    print("=" * 80)
    print(f"Testing actor with URL: {test_url}")
    print("=" * 80)
    
    # Test 1: includeSeller = false (Stage 1 - cheap)
    print("\nTEST 1: includeSeller=false (Stage 1 - Title only)")
    print("-" * 80)
    
    actor_input = {
        "startUrls": [{"url": test_url}],
        "includeSeller": False,
        "monitoringMode": True,
        "maxItems": 1,
        "minDelay": 5,
        "maxDelay": 10,
        "maxConcurrency": 1,
        "minConcurrency": 1,
        "maxRequestRetries": 2,
        "proxyConfiguration": {
            "useApifyProxy": True,
            "apifyProxyGroups": ["RESIDENTIAL"]
        }
    }
    
    try:
        run = client.actor("memo23/facebook-marketplace-cheerio").call(run_input=actor_input)
        dataset_id = run['defaultDatasetId']
        items = list(client.dataset(dataset_id).iterate_items())
        
        print(f"Returned {len(items)} items")
        
        if items:
            print("\nFirst item structure:")
            print(json.dumps(items[0], indent=2))
        else:
            print("No items returned!")
    except Exception as e:
        print(f"Error: {e}")
    
    print("\n" + "=" * 80)
    print("\nTEST 2: includeSeller=true (Stage 2 - Full details)")
    print("-" * 80)
    
    actor_input['includeSeller'] = True
    actor_input['monitoringMode'] = False
    
    try:
        run = client.actor("memo23/facebook-marketplace-cheerio").call(run_input=actor_input)
        dataset_id = run['defaultDatasetId']
        items = list(client.dataset(dataset_id).iterate_items())
        
        print(f"Returned {len(items)} items")
        
        if items:
            print("\nFirst item structure:")
            print(json.dumps(items[0], indent=2))
            
            # Show all top-level keys
            print("\nTop-level keys:")
            for key in items[0].keys():
                print(f"  - {key}")
        else:
            print("No items returned!")
    except Exception as e:
        print(f"Error: {e}")
    
    print("\n" + "=" * 80)


if __name__ == '__main__':
    test_actor_output()
