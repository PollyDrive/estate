#!/usr/bin/env python3
"""
Test script to verify two-stage filtering logic without actual API calls.
"""

import json
from src.property_parser import PropertyParser


def simulate_stage1_filtering():
    """Simulate Stage 1: Title-only filtering."""
    
    parser = PropertyParser()
    
    # Load criteria from config
    with open('config/config.json', 'r') as f:
        config = json.load(f)
    criterias = config.get('criterias', {})
    
    # Simulate titles from Stage 1 (no descriptions)
    # Based on REAL titles from database
    stage1_titles = [
        # Should PASS title filters (2BR, no stop words)
        "2 KT 1 Kamar Mandi - Rumah",
        "Yearly Rent Villa in Berawa 2 Bedrooms",
        "2 Beds 2 Baths -",
        "2 Beds 1 Bath - Apartment/Condo",
        "2BR villa 10 juta/month",
        
        # Should FAIL: wrong bedrooms
        "Studio 0 KM -",
        "1 Bed 1 Bath House",
        "3 Beds 3 Baths - House",
        "4 KT 2 KM - Rumah",
        "6 KT 3 KM - Rumah",
        
        # Should FAIL: stop words
        "Tanah singapadu dekat Ubud",
        "Rumah kos atau sewa",
        "Over kontrak seisinya salon",
        "2BR dijual AC WiFi - 10 juta",
        
        # Should FAIL: daily rental
        "2BR villa - 500k per day",
        
        # Should FAIL: too expensive
        "2BR villa - 20 juta",
        
        # Edge cases - should PASS (no bedroom info, will check on Stage 2)
        "Fo rent monthly house nusa dua",
        " de 2 chambres et 2 salles de bain",
    ]
    
    print("=" * 80)
    print("STAGE 1: Title-only Filtering Simulation")
    print("=" * 80)
    
    passed_count = 0
    failed_count = 0
    
    for i, title in enumerate(stage1_titles, 1):
        print(f"\n[{i}/{len(stage1_titles)}] Title: {title}")
        
        # Parse title only
        params = parser.parse(title)
        print(f"  Parsed: bedrooms={params.get('bedrooms')}, price={params.get('price')}, "
              f"kitchen={params.get('has_kitchen')}, AC={params.get('has_ac')}, "
              f"WiFi={params.get('has_wifi')}, term={params.get('rental_term')}, "
              f"stop_word={params.get('has_stop_word')}")
        
        # Check criteria
        passed, reason = parser.matches_criteria(params, criterias)
        
        if passed:
            print(f"  ✓ PASSED: {reason}")
            passed_count += 1
        else:
            print(f"  ✗ FILTERED: {reason}")
            failed_count += 1
    
    print("\n" + "=" * 80)
    print(f"STAGE 1 RESULTS:")
    print(f"  Total listings: {len(stage1_titles)}")
    print(f"  Passed filters: {passed_count} ({passed_count/len(stage1_titles)*100:.1f}%)")
    print(f"  Filtered out: {failed_count} ({failed_count/len(stage1_titles)*100:.1f}%)")
    print("=" * 80)
    
    print(f"\nCOST ANALYSIS:")
    print(f"  Old approach (all with full descriptions):")
    print(f"    - Items scraped: {len(stage1_titles)} with includeSeller=true")
    print(f"    - Estimated cost: ~$0.02 per 20 items = ${len(stage1_titles)/20*0.02:.3f}")
    print(f"")
    print(f"  New two-stage approach:")
    print(f"    - Stage 1: {len(stage1_titles)} titles (includeSeller=false)")
    print(f"    - Stage 1 cost: ~$0.01 per 100 items = ${len(stage1_titles)/100*0.01:.3f}")
    print(f"    - Stage 2: {passed_count} full details (includeSeller=true)")
    print(f"    - Stage 2 cost: ~$0.02 per 20 items = ${passed_count/20*0.02:.3f}")
    print(f"    - Total cost: ${len(stage1_titles)/100*0.01 + passed_count/20*0.02:.3f}")
    print(f"")
    
    old_cost = len(stage1_titles)/20*0.02
    new_cost = len(stage1_titles)/100*0.01 + passed_count/20*0.02
    savings = old_cost - new_cost
    savings_pct = savings/old_cost*100 if old_cost > 0 else 0
    
    print(f"  SAVINGS: ${savings:.3f} ({savings_pct:.1f}%)")
    print("=" * 80)


if __name__ == '__main__':
    simulate_stage1_filtering()
