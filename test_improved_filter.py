#!/usr/bin/env python3
"""Test improved Zhipu filter on problematic listings"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv
import json

sys.path.insert(0, str(Path(__file__).parent / 'src'))
from llm_filters import ZhipuFilter

load_dotenv()

with open('config/config.json', 'r', encoding='utf-8') as f:
    config = json.load(f)

zhipu_api_key = os.getenv('ZHIPU_API_KEY')
zhipu = ZhipuFilter(config, zhipu_api_key)

# Test cases from problematic listings
test_cases = [
    {
        "id": 510,
        "description": """Facilities
Working space
Wardrobe
Sitting area (in every room and in front of the room).
Smart TV Android
AC
Private bathroom (shower, water heater, wastafel, closet, tissue).
Private kitchen (cutleries, cooking ware, stove, microwave, sink, refrigerator, water dispenser).
Private dining area (Bada Room)
Free WiFi
Free parking lot
Daily cleaning""",
        "expected": False,
        "reason": "REJECT_BEDROOMS (no bedroom count mentioned)"
    },
    {
        "id": 509,
        "description": """2 bedroom private pool
2 bathroom
1 kitchen
Internet 200mbps
Parking""",
        "expected": True,
        "reason": "PASS (2 bedroom explicitly mentioned)"
    },
    {
        "id": 508,
        "description": "1 building 1 room with 4.5m x 5m, full AC, Internet, TV, safedeposit, bathroom with hotwater, bathtub. kitchen",
        "expected": False,
        "reason": "REJECT_BEDROOMS (1 room)"
    },
    {
        "id": "kos_test",
        "description": "Menerima kos perempuan dengan AC, WiFi, kamar mandi dalam",
        "expected": False,
        "reason": "REJECT_TYPE (kos)"
    },
    {
        "id": "warung_test",
        "description": "Di sewa tempat jualan / warung dengan parkir motor",
        "expected": False,
        "reason": "REJECT_TYPE (tempat jualan)"
    }
]

print("=" * 80)
print("TESTING IMPROVED ZHIPU FILTER")
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
