#!/usr/bin/env python3
import os
from dotenv import load_dotenv
from apify_client import ApifyClient
import json

load_dotenv()
client = ApifyClient(os.getenv('APIFY_API_KEY'))

run_id = 'eS9fXBPPyPqwBNjp5'
run = client.run(run_id).get()
dataset_id = run['defaultDatasetId']
items = list(client.dataset(dataset_id).iterate_items())

print(f"Total items: {len(items)}")
print("\nFirst item structure:")
print(json.dumps(items[0], indent=2, ensure_ascii=False)[:1000])
