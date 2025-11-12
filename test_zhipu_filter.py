#!/usr/bin/env python3
"""
Тест Zhipu фильтра с реальными примерами
"""

import os
import sys
import json
from pathlib import Path
from dotenv import load_dotenv

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from llm_filters import ZhipuFilter

# Load environment
load_dotenv()

# Load config
with open('config/config.json', 'r', encoding='utf-8') as f:
    config = json.load(f)

# Get API key
zhipu_api_key = os.getenv('ZHIPU_API_KEY')

if not zhipu_api_key:
    print("❌ ZHIPU_API_KEY not found!")
    sys.exit(1)

# Initialize filter
print("🤖 Initializing Zhipu filter...\n")
zhipu_filter = ZhipuFilter(config, zhipu_api_key)

# Test cases
test_cases = [
    {
        "description": "Villa 2BR in Ubud, kitchen, pool, 14jt/month fully furnished",
        "expected": "PASS"
    },
    {
        "description": "Beautiful 1 bedroom apartment in Canggu, 10jt/month",
        "expected": "REJECT_BEDROOMS"
    },
    {
        "description": "Land for rent, 400m2, 7 million per year",
        "expected": "REJECT_TYPE"
    },
    {
        "description": "2BR Villa, 150jt/year only, no monthly option",
        "expected": "REJECT_TERM"
    },
    {
        "description": "Modern 2BR villa, unfurnished, 12jt/month",
        "expected": "REJECT_FURNITURE"
    },
    {
        "description": "Luxury 2BR villa with pool, 25 million IDR per month",
        "expected": "REJECT_PRICE"
    },
    {
        "description": "2 bedroom house with kitchen, monthly rent 16jt",
        "expected": "PASS"
    },
    {
        "description": "Nice 2BR villa, 17 million per month",
        "expected": "REJECT_PRICE"
    },
    {
        "description": "Dijual villa 2BR, good price 200jt",
        "expected": "REJECT_TYPE"
    },
    {
        "description": "Kos AC + isian, 2jt/month",
        "expected": "REJECT_TYPE"
    }
]

print("=" * 80)
print("Testing Zhipu Filter")
print("=" * 80)

passed = 0
failed = 0

for i, test in enumerate(test_cases, 1):
    print(f"\n📝 Test {i}:")
    print(f"Description: {test['description'][:60]}...")
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
