#!/usr/bin/env python3
"""Test the fixed Zhipu filter on problematic listings"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv
import json

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from llm_filters import ZhipuFilter

# Load environment
load_dotenv()

# Load config
with open('config/config.json', 'r', encoding='utf-8') as f:
    config = json.load(f)

zhipu_api_key = os.getenv('ZHIPU_API_KEY')
zhipu = ZhipuFilter(config, zhipu_api_key)

# Test cases from problematic listings
test_cases = [
    {
        "id": 26,
        "description": """Available now 2 bedroom house full furniture for long term rent 
Good location, friendly community.
Have security, gym, pool, pilates, coffee shop, Pepito.
180 mln/ year""",
        "expected": False,
        "reason": "REJECT_TERM (yearly only)"
    },
    {
        "id": 141,
        "description": """Beautiful house in Ubud for rent:

With rice fields view
Motorcycle road only

2 Beds 2 Baths - House

All included:
Wifi
Cleaning 3x a week
Electric
Bedsheets laundry
Water galon

Available: now for monthly or yearly

Please DM for more details,
Thank you""",
        "expected": True,
        "reason": "PASS (monthly or yearly = monthly option available)"
    },
    {
        "id": 422,
        "description": """✨Brand new villa in Cemagi only 5min to the beach 🏝️
- 2 Bedrooms
- 2 Bathrooms
- Enclosed living room
- Enclosed kitchen
- Private pool
- Garage
- Location is 5 min to Cemagi beach by scooter.
- No construction, no noise bikes, quiet neighbourhood.
- Price yearly 210 mill upfront.
Includes: pool cleaning and wifi installation(Global Extreme)
We have 2 units available now, just finished build.
Kindly contact for more information.
Thank you 🙏🏽""",
        "expected": False,
        "reason": "REJECT_TERM (yearly only)"
    },
    {
        "id": 424,
        "description": """Beautiful house in Ubud for rent:
With rice fields view
Motorcycle road only
2 Beds 2 Baths - House
All included:
Wifi
Cleaning 3x a week
Electric
Bedsheets laundry
Water galon
Available: now for monthly or yearly
Please DM for more details,
Thank you""",
        "expected": True,
        "reason": "PASS but price 197 mln should be >16jt"
    }
]

print("=" * 80)
print("TESTING FIXED ZHIPU FILTER")
print("=" * 80)

passed_tests = 0
failed_tests = 0

for test in test_cases:
    print(f"\n{'=' * 80}")
    print(f"Test ID {test['id']}: {test['reason']}")
    print(f"Expected: {'PASS' if test['expected'] else 'REJECT'}")
    print(f"{'=' * 80}")
    
    passed, reason = zhipu.filter(test['description'])
    
    result = "✅ CORRECT" if passed == test['expected'] else "❌ WRONG"
    print(f"Got: {'PASS' if passed else 'REJECT'} - {reason}")
    print(f"{result}")
    
    if passed == test['expected']:
        passed_tests += 1
    else:
        failed_tests += 1

print(f"\n{'=' * 80}")
print(f"RESULTS: {passed_tests}/{len(test_cases)} tests passed")
print(f"{'=' * 80}")
