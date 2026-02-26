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
            r'(?<!\d)(\d{1,2})[\s-]*(?:br|bedroom|bedrooms|kamar tidur)\b',
            r'(?<!\d)(\d{1,2})[\s-]*kt\b',   # KT = Kamar Tidur
            r'(?<!\d)(\d{1,2})[\s-]*beds?\b',
            r'\bstudio\b',  # Special case: 0 bedrooms
        ]
        # Spelled-out English numbers for bedroom counts (e.g. "Four bedroom villa")
        self._word_to_num = {
            'one': 1, 'two': 2, 'three': 3, 'four': 4, 'five': 5,
            'six': 6, 'seven': 7, 'eight': 8, 'nine': 9, 'ten': 10,
        }
        self._word_bedroom_pattern = re.compile(
            r'\b(one|two|three|four|five|six|seven|eight|nine|ten)[\s-]*(?:bed(?:room)?s?|br)\b',
            re.IGNORECASE
        )
        
        # USD → IDR conversion rate (approximate, for filtering purposes)
        self._usd_to_idr = 16_300

        # Price patterns — USD detected separately, then IDR
        self.usd_price_pattern = re.compile(
            r'(?:usd|\$|us\$)\s*(\d[\d,]*(?:\.\d+)?)'  # $4,000 / USD 4000 / US$3,500
            r'|(\d[\d,]*(?:\.\d+)?)\s*(?:usd|dollars?)',  # 4000 USD / 4000 dollars
            re.IGNORECASE
        )
        self.price_patterns = [
            # 3.5 million, 10 juta, 3,5 juta (with decimal point or comma)
            r'(\d+[.,]\d+)\s*(?:jt|juta|million|m)\b',
            # Whole number with jt/juta/million (10 juta, 5jt)
            r'(\d+)\s*(?:jt|juta|million|m)\b',
            # Special formats: 180mln, 250mio, 90mill, 25mil
            r'(\d+)\s*(?:mln|mio|mill|mil)\b',
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
        
        # Known locations in Bali (common areas)
        self.known_locations = [
            # Allowed locations (high priority)
            'Ubud', 'Abiansemal', 'Singakerta', 'Mengwi', 'Gianyar',
            # Other popular locations
            'Canggu', 'Seminyak', 'Kuta', 'Legian', 'Sanur', 
            'Denpasar', 'Uluwatu', 'Jimbaran', 'Nusa Dua',
            'Pererenan', 'Berawa', 'Echo Beach', 'Batu Bolong',
            'Umalas', 'Kerobokan', 'Petitenget',
            'Bingin', 'Padang Padang', 'Balangan',
            'Candidasa', 'Amed', 'Lovina', 'Singaraja',
            'Tabanan', 'Kediri', 'Munggu', 'Tanah Lot',
            'Tegallalang', 'Payangan', 'Petulu', 'Mas', 'Lodtunduh',
            'Sukawati', 'Celuk', 'Batuan', 'Blahbatuh'
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
        """Extract number of bedrooms with sanity guard."""
        def _sane(n: int) -> bool:
            # Guard against postal codes / IDs being mistaken for bedroom count.
            return 0 <= n <= 20

        for pattern in self.bedroom_patterns:
            for match in re.finditer(pattern, text, re.IGNORECASE):
                if pattern == r'\bstudio\b':
                    return 0
                try:
                    n = int(match.group(1))
                except (ValueError, IndexError):
                    continue
                if _sane(n):
                    return n
        # Fallback: spelled-out numbers ("Four bedroom", "two-bed", etc.)
        match = self._word_bedroom_pattern.search(text)
        if match:
            return self._word_to_num.get(match.group(1).lower())
        return None
    
    def _extract_price(self, text: str) -> Optional[float]:
        """Extract price in IDR (returns monthly price).
        USD prices are converted to IDR using self._usd_to_idr rate so that
        non-Bali listings with dollar prices will exceed IDR price_max thresholds.
        """
        # Normalize broken number formatting like "20. 000,000" -> "20.000,000"
        text = re.sub(r'(?<=\d)([.,])\s+(?=\d)', r'\1', text or "")

        monthly_indicators = [
            r'monthly',
            r'/month',
            r'per month',
            r'\bmonth\b',
            r'/mo\b',
            r'bulanan',
            r'/bulan',
            r'per bulan',
            r'\bbln\b',
        ]
        yearly_indicators = [
            r'yearly',
            r'/year',
            r'per year',
            r'\byear\b',
            r'tahunan',
            r'/tahun',
            r'per tahun',
            r'/yr',
        ]

        def _is_yearly_context(ctx: str) -> bool:
            has_yearly = any(re.search(ind, ctx) for ind in yearly_indicators)
            has_monthly = any(re.search(ind, ctx) for ind in monthly_indicators)
            # Do NOT divide by 12 when both monthly and yearly are present nearby.
            return has_yearly and not has_monthly

        # ── USD detection (checked first to avoid IDR patterns grabbing the number) ──
        m = self.usd_price_pattern.search(text)
        if m:
            raw = (m.group(1) or m.group(2) or '').replace(',', '')
            try:
                usd_amount = float(raw)
                idr_price = usd_amount * self._usd_to_idr
                # Apply yearly→monthly conversion if needed
                context = text[max(0, m.start()-50):min(len(text), m.end()+50)].lower()
                if _is_yearly_context(context):
                    idr_price /= 12
                return idr_price
            except (ValueError, TypeError):
                pass

        for pattern in self.price_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    price_str = match.group(1)
                    matched_text = match.group(0).lower()
                    
                    # Handle million/juta formats including mln, mio, mill
                    if ('jt' in matched_text or 'juta' in matched_text or 'million' in matched_text or 
                        'mln' in matched_text or 'mio' in matched_text or 'mill' in matched_text or 'mil' in matched_text or
                        matched_text.strip().endswith('m')):
                        # Replace comma with dot for decimal (3,5 → 3.5)
                        price_str_normalized = price_str.replace(',', '.')
                        price = float(price_str_normalized) * 1_000_000
                    else:
                        # Remove separators for full numbers like 10.000.000 or 10,000,000
                        price_str_clean = price_str.replace(',', '').replace('.', '')
                        price = float(price_str_clean)
                        
                        # This logic was too ambiguous and caused errors.
                        # A number like "17" was incorrectly interpreted as 17 million.
                        # The logic now relies on explicit markers like "jt" or "juta".
                        # if price < 100:
                        #     price = price * 1_000_000
                    
                    # Check if price context indicates yearly/annual rental
                    # Look for yearly indicators within 50 chars before/after the price
                    start_pos = max(0, match.start() - 50)
                    end_pos = min(len(text), match.end() + 50)
                    context = text[start_pos:end_pos].lower()
                    
                    # Convert yearly to monthly if needed
                    if _is_yearly_context(context):
                        price = price / 12
                    
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
    
    def extract_location(self, text: str) -> Optional[str]:
        """
        Extract location from text.
        Looks for patterns like "in Ubud", "at Canggu", location names.
        
        Args:
            text: Description text
            
        Returns:
            Extracted location name or None
        """
        if not text:
            return None
        
        text_lower = text.lower()
        
        # Pattern 1: "in Location" (most explicit)
        for location in self.known_locations:
            location_lower = location.lower()
            
            # Try explicit patterns first
            patterns = [
                rf'\bin\s+{re.escape(location_lower)}\b',
                rf'\bat\s+{re.escape(location_lower)}\b',
                rf'\bdi\s+{re.escape(location_lower)}\b',  # Indonesian "di" = in/at
                rf'{re.escape(location_lower)}\s+area\b',
                rf'{re.escape(location_lower)}\s+location\b',
            ]
            
            for pattern in patterns:
                if re.search(pattern, text_lower):
                    return location
        
        # Pattern 2: Just location name mentioned (less confident)
        # Only if it appears at start or after newline (likely location info)
        for location in self.known_locations:
            location_lower = location.lower()
            
            # Check if location appears at start of text or line
            pattern = rf'(?:^|\n)\s*{re.escape(location_lower)}\b'
            if re.search(pattern, text_lower):
                return location
        
        return None
    
    def extract_title_from_description(self, text: str, max_length: int = 100) -> str:
        """
        Extract a meaningful title from description text.
        Takes first sentence or first N characters.
        
        Args:
            text: Description text
            max_length: Maximum title length (default 100)
            
        Returns:
            Extracted title string
        """
        if not text:
            return ""
        
        # Clean text
        text = text.strip()
        
        # Try to get first sentence (ending with . ! ? or newline)
        sentence_match = re.match(r'^([^.!?\n]+[.!?]?)', text)
        if sentence_match:
            title = sentence_match.group(1).strip()
        else:
            # If no sentence delimiters, take first line or max_length chars
            lines = text.split('\n')
            title = lines[0].strip() if lines else text
        
        # Truncate if too long
        if len(title) > max_length:
            title = title[:max_length].rsplit(' ', 1)[0] + '...'
        
        return title
    
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
    
    def matches_criteria(self, params: Dict, criteria: Dict, stage: int = 1) -> Tuple[bool, str]:
        """
        Check if parsed parameters match search criteria.
        
        Args:
            params: Parsed parameters
            criteria: Search criteria from config
            stage: Filter stage (1=basic, 2=detailed with description)
            
        Returns:
            Tuple of (matches: bool, reason: str)
        """
        # 1. Check stop words (land/sale)
        if params.get('has_stop_word'):
            return False, "Stop word: tanah/dijual/sale"
        
        # 2. Check rental term (reject daily/weekly/nightly)
        if params.get('rental_term') in ['daily', 'weekly']:
            return False, f"Rental term: {params['rental_term']}"
        
        # 3. Check bedrooms: reject only explicit low BR counts; skip if not in criteria
        bedrooms_min = criteria.get('bedrooms_min')
        bedrooms = params.get('bedrooms')
        if bedrooms_min is not None and bedrooms is not None and bedrooms < bedrooms_min:
            return False, f"Bedrooms: {bedrooms} (need {bedrooms_min}+)"

        # 4. Check price range: skip if not in criteria
        price = params.get('price')
        max_price = criteria.get('price_max')
        if price and max_price is not None and price > max_price:
            return False, f"Price {price:,.0f} > {max_price:,.0f}"
        
        # 5. Kitchen check REMOVED - too many false negatives
        # Kitchen info is often mentioned but not parsed correctly
        # Will be manually checked by user
        
        # 6. Check AC (MOVED TO STAGE 3 - LLM)
        # AC info often only in full description
        
        # 7. Check WiFi (MOVED TO STAGE 3 - LLM)
        # WiFi info often only in full description
        
        return True, "Passed filters"
