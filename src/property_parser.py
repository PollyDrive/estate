"""
Property parameters parser for extracting structured data from listings.
Extracts: bedrooms, price, kitchen type, AC, WiFi, utilities, furniture.
"""

import re
import logging
from typing import Dict, Optional, List, Tuple

logger = logging.getLogger(__name__)


class PropertyParser:
    """Extract structured parameters from property descriptions."""
    
    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize parser with regex patterns.
        
        Args:
            config: Optional configuration dict with stop_words
        """
        
        # Bedroom patterns
        self.bedroom_patterns = [
            r'(\d+)\s*(?:br|bedroom|bedrooms|kamar tidur)',
            r'(\d+)\s*kt\b',  # KT = Kamar Tidur (Indonesian for bedroom)
            r'(\d+)\s*bed',
            r'studio',  # Special case: 0 bedrooms
        ]
        
        # Price patterns (IDR)
        self.price_patterns = [
            # 3.5 million, 10 juta, 3,5 juta (with decimal point or comma)
            r'(\d+[.,]\d+)\s*(?:jt|juta|million|m)\b',
            # Whole number with jt/juta/million (10 juta, 5jt)
            r'(\d+)\s*(?:jt|juta|million|m)\b',
            # IDR/Rp followed by number (treat as millions if < 100)
            r'(?:rp|idr)[\s.]?(\d+(?:[.,]\d{3})*(?:[.,]\d+)?)\s*(?:jt|juta|jt/bln|juta/bulan|million|m)?',
            # Any number with thousand separators (full format like 10.000.000)
            r'(\d{1,3}(?:[.,]\d{3})+)',
        ]
        
        # Kitchen type patterns
        self.kitchen_patterns = {
            'enclosed': [
                r'closed kitchen',
                r'enclosed kitchen',
                r'indoor kitchen',
                r'private kitchen',
                r'full kitchen',
                r'western kitchen',
                r'dapur tertutup',
                r'dapur indoor',
            ],
            'outdoor': [
                r'outdoor kitchen',
                r'open kitchen',
                r'dapur outdoor',
                r'dapur terbuka',
            ],
            'shared': [
                r'shared kitchen',
                r'common kitchen',
                r'dapur bersama',
            ],
            'kitchenette': [
                r'kitchenette',
                r'mini kitchen',
                r'small kitchen',
            ],
            'none': [
                r'no kitchen',
                r'tanpa dapur',
                r'without kitchen',
            ]
        }
        
        # Amenities patterns
        self.amenity_patterns = {
            'ac': [
                r'\bac\b',
                r'air conditioning',
                r'air conditioner',
                r'air con',
                r'a/c',
                r'aircon',
            ],
            'wifi': [
                r'\bwifi\b',
                r'wi-fi',
                r'internet',
                r'wireless',
            ],
            'pool': [
                r'\bpool\b',
                r'swimming pool',
                r'kolam renang',
            ],
            'parking': [
                r'parking',
                r'parkir',
                r'garage',
            ]
        }
        
        # Negative amenity patterns (explicitly NO amenity)
        self.negative_amenity_patterns = {
            'no_ac': [
                r'no ac\b',
                r'no air con',
                r'fan only',
                r'kipas saja',
                r'tanpa ac',
            ],
            'no_wifi': [
                r'no wifi',
                r'no internet',
                r'tanpa wifi',
                r'tanpa internet',
            ]
        }
        
        # Utilities patterns
        self.utilities_patterns = {
            'included': [
                r'(?:bills?|utilities?|listrik|air)\s+(?:included|include|inc|sudah termasuk)',
                r'all\s+(?:bills?|utilities?)\s+included',
                r'include\s+(?:bills?|utilities?)',
            ],
            'excluded': [
                r'(?:bills?|utilities?|listrik|air)\s+(?:excluded|exclude|exc|belum termasuk|tidak termasuk)',
                r'(?:bills?|utilities?)\s+(?:not included|separate|extra)',
                r'plus\s+(?:bills?|utilities?)',
            ]
        }
        
        # Furniture patterns
        self.furniture_patterns = {
            'fully': [
                r'fully furnished',
                r'full furniture',
                r'completely furnished',
                r'lengkap perabotan',
            ],
            'semi': [
                r'semi furnished',
                r'partial furniture',
                r'sebagian perabotan',
            ],
            'unfurnished': [
                r'unfurnished',
                r'no furniture',
                r'tanpa perabotan',
            ]
        }
        
        # Rental term patterns
        self.term_patterns = {
            'monthly': [
                r'(?:per|/)\s*(?:month|bulan|bln)',
                r'monthly',
                r'bulanan',
            ],
            'yearly': [
                r'(?:per|/)\s*(?:year|tahun|thn)',
                r'yearly',
                r'tahunan',
            ],
            'daily': [
                r'(?:per|/)\s*(?:day|hari)',
                r'daily',
                r'harian',
                r'nightly',
            ],
            'weekly': [
                r'(?:per|/)\s*(?:week|minggu)',
                r'weekly',
                r'mingguan',
            ]
        }
        
        # Stop words - instant reject patterns (title filtering)
        # Load from config if provided, otherwise use defaults
        if config and 'filters' in config and 'stop_words' in config['filters']:
            # Convert config stop words to regex patterns (case-insensitive matching)
            self.stop_words = [
                r'\b' + re.escape(word.lower()) + r'\b' 
                for word in config['filters']['stop_words']
            ]
            logger.info(f"Loaded {len(self.stop_words)} stop words from config")
        else:
            # Default hardcoded stop words
            self.stop_words = [
                r'\btanah\b',  # Land rental (not property)
                r'dikontrakan tanah',  # Land for rent
                r'\bdijual\b',  # For sale (not rent)
                r'for sale',
                r'\bsale\b',
                r'\bstudio\b',  # Studio apartments (0 bedrooms)
                r'\b0\s*km\b',  # 0 bathrooms (weird listings)
                r'yearly',  # Yearly rental (not monthly)
                r'tahunan',  # Tahunan = yearly in Indonesian
                r'over kontrak',  # Contract transfer (commercial/salon)
                r'\bsalon\b',  # Salon (commercial property)
                r'\bkos\b',  # Kos (hostel/boarding house)
                r'\bkost\b',  # Kost (variant spelling)
            ]
    
    def parse(self, text: str) -> Dict:
        """
        Parse property listing text and extract parameters.
        
        Args:
            text: Listing description text
            
        Returns:
            Dict with extracted parameters
        """
        if not text:
            return {}
        
        text_lower = text.lower()
        
        # Check for stop words first
        has_stop_word = self._check_stop_words(text_lower)
        
        return {
            # Critical filters (title-level)
            'bedrooms': self._extract_bedrooms(text_lower),
            'price': self._extract_price(text_lower),
            'has_kitchen': self._has_kitchen_mention(text_lower),
            'has_ac': self._check_amenity(text_lower, 'ac'),
            'has_wifi': self._check_amenity(text_lower, 'wifi'),
            'rental_term': self._extract_rental_term(text_lower),
            'has_stop_word': has_stop_word,
        }
    
    def _extract_bedrooms(self, text: str) -> Optional[int]:
        """Extract number of bedrooms."""
        for pattern in self.bedroom_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                if pattern == r'studio':
                    return 0
                try:
                    return int(match.group(1))
                except (ValueError, IndexError):
                    continue
        return None
    
    def _extract_price(self, text: str) -> Optional[float]:
        """Extract price in IDR (returns actual value, not millions)."""
        for pattern in self.price_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    price_str = match.group(1)
                    matched_text = match.group(0).lower()
                    
                    # Handle million/juta formats
                    if 'jt' in matched_text or 'juta' in matched_text or 'million' in matched_text or matched_text.strip().endswith('m'):
                        # Replace comma with dot for decimal (3,5 → 3.5)
                        price_str_normalized = price_str.replace(',', '.')
                        price = float(price_str_normalized) * 1_000_000
                    else:
                        # Remove separators for full numbers like 10.000.000 or 10,000,000
                        price_str_clean = price_str.replace(',', '').replace('.', '')
                        price = float(price_str_clean)
                        
                        # If resulting number is small (< 100), likely in millions
                        if price < 100:
                            price = price * 1_000_000
                    
                    return price
                except (ValueError, IndexError):
                    continue
        return None
    
    def _extract_kitchen_type(self, text: str) -> Optional[str]:
        """Extract kitchen type."""
        for kitchen_type, patterns in self.kitchen_patterns.items():
            for pattern in patterns:
                if re.search(pattern, text, re.IGNORECASE):
                    return kitchen_type
        
        # Default: check if kitchen mentioned at all
        if re.search(r'\b(?:kitchen|dapur|kitchenette)\b', text, re.IGNORECASE):
            return 'unknown'
        
        return None
    
    def _check_amenity(self, text: str, amenity: str) -> bool:
        """Check if amenity is mentioned."""
        if amenity not in self.amenity_patterns:
            return False
        
        # Check for explicit negatives first (no AC, no WiFi, fan only)
        negative_key = f'no_{amenity}'
        if negative_key in self.negative_amenity_patterns:
            for pattern in self.negative_amenity_patterns[negative_key]:
                if re.search(pattern, text, re.IGNORECASE):
                    return False  # Explicitly NO amenity
        
        # Check for positive mentions
        for pattern in self.amenity_patterns[amenity]:
            if re.search(pattern, text, re.IGNORECASE):
                return True
        return False
    
    def _has_kitchen_mention(self, text: str) -> bool:
        """Check if kitchen is mentioned at all (simple check)."""
        kitchen_keywords = [r'\bkitchen\b', r'\bdapur\b', r'\bkitchenette\b']
        for pattern in kitchen_keywords:
            if re.search(pattern, text, re.IGNORECASE):
                return True
        return False
    
    def _extract_utilities(self, text: str) -> Optional[str]:
        """Extract utilities status (included/excluded)."""
        for status, patterns in self.utilities_patterns.items():
            for pattern in patterns:
                if re.search(pattern, text, re.IGNORECASE):
                    return status
        return None
    
    def _extract_furniture(self, text: str) -> Optional[str]:
        """Extract furniture status."""
        for status, patterns in self.furniture_patterns.items():
            for pattern in patterns:
                if re.search(pattern, text, re.IGNORECASE):
                    return status
        return None
    
    def _extract_rental_term(self, text: str) -> Optional[str]:
        """Extract rental term (monthly/yearly/daily/weekly)."""
        for term, patterns in self.term_patterns.items():
            for pattern in patterns:
                if re.search(pattern, text, re.IGNORECASE):
                    return term
        return None
    
    def _check_stop_words(self, text: str) -> bool:
        """Check if text contains stop words (land rental, etc)."""
        for pattern in self.stop_words:
            if re.search(pattern, text, re.IGNORECASE):
                return True
        return False
    
    def extract_phone_numbers(self, text: str) -> List[str]:
        """
        Extract Indonesian phone numbers.
        
        Args:
            text: Text to search for phone numbers
            
        Returns:
            List of found phone numbers
        """
        patterns = [
            r'\+?62\s?8\d{2}[\s-]?\d{3,4}[\s-]?\d{3,4}',
            r'08\d{2}[\s-]?\d{3,4}[\s-]?\d{3,4}',
            r'\+?62[\s-]?8\d{9,11}',
        ]
        
        phones = []
        for pattern in patterns:
            matches = re.findall(pattern, text)
            phones.extend(matches)
        
        # Clean and deduplicate
        phones = [p.strip().replace(' ', '').replace('-', '') for p in phones]
        return list(set(phones))
    
    def matches_criteria(self, params: Dict, criteria: Dict) -> Tuple[bool, str]:
        """
        Check if parsed parameters match search criteria.
        
        Args:
            params: Parsed parameters
            criteria: Search criteria from config
            
        Returns:
            Tuple of (matches: bool, reason: str)
        """
        # TITLE-LEVEL FILTERS (fast, no deep parsing)
        
        # 1. Check stop words (land/sale)
        if params.get('has_stop_word'):
            return False, "Stop word: tanah/dijual/sale"
        
        # 2. Check rental term (reject daily/weekly/nightly)
        if params.get('rental_term') in ['daily', 'weekly']:
            return False, f"Rental term: {params['rental_term']}"
        
        # 3. Check bedrooms (only 2BR allowed)
        bedrooms = params.get('bedrooms')
        if bedrooms is not None and bedrooms != 2:
            return False, f"Bedrooms: {bedrooms} (need 2)"
        
        # 4. Check price range
        price = params.get('price')
        
        if price:
            # For 2BR: 4M - 14M IDR
            max_price = criteria.get('default_price', {}).get('max', 14000000)
            if price > max_price:
                return False, f"Price {price:,.0f} > {max_price:,.0f}"
        
        # 5. Check kitchen mention (MOVED TO STAGE 2)
        # Kitchen info often only in full description, not title
        # if not params.get('has_kitchen'):
        #     return False, "No kitchen mentioned"
        
        # 6. Check AC (MOVED TO STAGE 2)
        # AC info often only in full description, not title
        # if not params.get('has_ac'):
        #     return False, "No AC"
        
        # 7. Check WiFi (MOVED TO STAGE 2)
        # WiFi info often only in full description, not title
        # if not params.get('has_wifi'):
        #     return False, "No WiFi"
        
        return True, "Passed title filters"
