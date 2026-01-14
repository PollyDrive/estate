#!/usr/bin/env python3
"""
Test location extraction from descriptions.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from property_parser import PropertyParser

# Test cases
test_cases = [
    ("Beautiful 2BR villa in Ubud with pool and garden", "Ubud"),
    ("Villa at Canggu, 3BR, close to beach", "Canggu"),
    ("Cozy house di Seminyak, fully furnished", "Seminyak"),
    ("Ubud area - Monthly rental villa 2BR", "Ubud"),
    ("Villa near Ubud center, quiet location", None),  # "near" не подходит
    ("Pererenan villa with rice field view", "Pererenan"),
    ("2BR in Abiansemal, perfect for family", "Abiansemal"),
    ("Tegallalang\nBeautiful villa for rent", "Tegallalang"),
    ("Villa 30 min from Seminyak", None),  # Расстояние, не локация
    ("Monthly rent villa, no location mentioned", None),
]

def main():
    parser = PropertyParser()
    
    print("=" * 80)
    print("LOCATION EXTRACTION TEST")
    print("=" * 80)
    
    passed = 0
    failed = 0
    
    for text, expected in test_cases:
        result = parser.extract_location(text)
        status = "✓" if result == expected else "✗"
        
        if result == expected:
            passed += 1
        else:
            failed += 1
        
        print(f"\n{status} Text: {text[:60]}...")
        print(f"  Expected: {expected}")
        print(f"  Got:      {result}")
    
    print("\n" + "=" * 80)
    print(f"RESULTS: {passed} passed, {failed} failed")
    print("=" * 80)

if __name__ == '__main__':
    main()
