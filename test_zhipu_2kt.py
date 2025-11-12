#!/usr/bin/env python3
"""
Тест фильтра Zhipu для 2KT объявлений
"""

import os
import sys
import json
from pathlib import Path
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent / 'src'))
from llm_filters import ZhipuFilter

load_dotenv()

with open('config/config.json', 'r', encoding='utf-8') as f:
    config = json.load(f)

zhipu_api_key = os.getenv('ZHIPU_API_KEY')
zhipu_filter = ZhipuFilter(config, zhipu_api_key)

# Test cases from real 2KT listings
test_cases = [
    {
        "description": "2 KT 1 Kamar Mandi - Rumah, sewa bulanan 12jt, fully furnished",
        "expected": "PASS",
        "note": "2KT = 2 bedrooms, monthly rent"
    },
    {
        "description": "2 KT 2 KM - Rumah, lokasi Ubud, 15 juta per bulan",
        "expected": "PASS",
        "note": "2KT = 2 bedrooms"
    },
    {
        "description": "3 KT 2 KM - Rumah, bulanan 14jt",
        "expected": "REJECT_BEDROOMS",
        "note": "3KT = 3 bedrooms (reject)"
    },
    {
        "description": "1 KT 1 KM - Rumah, sewa bulanan 8jt",
        "expected": "REJECT_BEDROOMS",
        "note": "1KT = 1 bedroom (reject)"
    },
    {
        "description": "2 KT 1 Kamar Mandi - Rumah, sewa tahunan saja 120jt",
        "expected": "REJECT_TERM",
        "note": "yearly only"
    },
    {
        "description": "2 BR villa in Ubud, monthly rent 13 million IDR",
        "expected": "PASS",
        "note": "2BR = 2 bedrooms"
    }
]

print("=" * 80)
print("Testing Zhipu Filter for 2KT/2BR Listings")
print("=" * 80)

passed = 0
failed = 0

for i, test in enumerate(test_cases, 1):
    print(f"\n📝 Test {i}: {test['note']}")
    print(f"Description: {test['description'][:80]}...")
    print(f"Expected: {test['expected']}")
    
    result, reason = zhipu_filter.filter(test['description'])
    
    print(f"Got: {reason}")
    
    if test['expected'] == 'PASS':
        if result and reason == "Passed all rules":
            print("✅ CORRECT")
            passed += 1
        else:
            print("❌ INCORRECT")
            failed += 1
    else:
        if not result and reason == test['expected']:
            print("✅ CORRECT")
            passed += 1
        else:
            print("❌ INCORRECT")
            failed += 1

print("\n" + "=" * 80)
print(f"Results: {passed} passed, {failed} failed out of {len(test_cases)} tests")
print("=" * 80)
