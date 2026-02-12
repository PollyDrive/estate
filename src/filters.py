import re
import logging
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class Level0Filter:
    """
    Level 0 filter: Hard filters using regex and keyword matching.
    Filters by price range, stop words, required words, and extracts phone numbers.
    Supports dynamic price ranges based on number of bedrooms.
    """
    
    def __init__(self, config: Dict):
        """
        Initialize Level 0 filter with configuration.
        
        Args:
            config: Configuration dictionary with filter settings
        """
        # Price rules by bedrooms (from criterias.json)
        self.price_rules = config.get('criterias', {}).get('price_rules', [])
        self.default_price = config.get('criterias', {}).get('default_price', {})
        self.bedrooms_min = config.get('criterias', {}).get('bedrooms_min', 4)
        
        self.stop_words = [word.lower() for word in config['filters']['stop_words']]
        self.stop_locations = [loc.lower() for loc in config['filters']['stop_locations']]
        self.required_words = [word.lower() for word in config['filters']['required_words']]
        self.phone_patterns = [re.compile(pattern) for pattern in config['filters']['phone_regex']]
    
    def extract_price(self, price_str: str) -> Optional[int]:
        """
        Extract numeric price from price string.
        
        Args:
            price_str: Price string (e.g., "Rp 5,000,000", "IDR 4,500,000 per month")
            
        Returns:
            Price as integer or None if extraction fails
        """
        if not price_str:
            return None
        
        # Extract only digits from the string
        # This handles: "Rp5,000,000", "IDR 4,500,000 per month", "Rp 5.000.000", etc.
        digits = re.findall(r'\d+', price_str)
        
        if not digits:
            logger.warning(f"Could not extract price from: {price_str}")
            return None
        
        # Join all digits (handles cases like "5,000,000" -> "5000000")
        try:
            price = int(''.join(digits))
            return price
        except ValueError:
            logger.warning(f"Could not convert to int: {price_str}")
            return None
    
    def extract_bedrooms(self, title: str, description: str) -> Optional[int]:
        """
        Extract number of bedrooms from title and description.
        
        Args:
            title: Listing title
            description: Listing description
            
        Returns:
            Number of bedrooms or None if not found
        """
        text = f"{title} {description}".lower()
        
        # Patterns to match bedroom count
        patterns = [
            r'(\d+)\s*(?:bed(?:room)?s?|br|kamar)',
            r'(?:bed(?:room)?s?|br|kamar)\s*[:\s]*(\d+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                try:
                    bedrooms = int(match.group(1))
                    logger.info(f"Found {bedrooms} bedroom(s)")
                    return bedrooms
                except (ValueError, IndexError):
                    continue
        
        logger.info("Could not extract bedroom count")
        return None
    
    def get_price_range(self, bedrooms: Optional[int]) -> Tuple[int, int]:
        """
        Get price range based on number of bedrooms.
        
        Args:
            bedrooms: Number of bedrooms (or None)
            
        Returns:
            Tuple of (min_price, max_price)
        """
        if bedrooms is not None:
            # Find matching price rule
            for rule in self.price_rules:
                if rule['bedrooms'] == bedrooms:
                    return (rule['min_price'], rule['max_price'])
        
        # Return default range if no match
        return (self.default_price['min'], self.default_price['max'])
    
    def check_price_range(self, price_str: str, title: str = "", description: str = "") -> bool:
        """
        Check if price is within acceptable range based on bedrooms.
        
        Args:
            price_str: Price string
            title: Listing title (optional, for bedroom detection)
            description: Listing description (optional, for bedroom detection)
            
        Returns:
            True if price is in range, False otherwise
        """
        price = self.extract_price(price_str)
        
        if price is None:
            logger.warning(f"Price extraction failed for: {price_str}")
            return False
        
        # Extract bedrooms and get appropriate price range
        bedrooms = self.extract_bedrooms(title, description)
        min_price, max_price = self.get_price_range(bedrooms)
        
        in_range = min_price <= price <= max_price
        
        if bedrooms:
            logger.info(f"Price check for {bedrooms} BR: {price} IDR (range: {min_price}-{max_price} IDR)")
        else:
            logger.info(f"Price check (default): {price} IDR (range: {min_price}-{max_price} IDR)")
        
        if not in_range:
            logger.info(f"Price {price} out of range ({min_price}-{max_price})")
        
        return in_range
    
    def check_stop_words(self, title: str, description: str) -> bool:
        """
        Check if listing contains any stop words or stop locations.
        
        Args:
            title: Listing title
            description: Listing description
            
        Returns:
            True if NO stop words/locations found (pass), False if found (fail)
        """
        text = f"{title} {description}".lower()
        
        # Check stop words
        for stop_word in self.stop_words:
            if stop_word in text:
                logger.info(f"Stop word found: '{stop_word}'")
                return False
        
        # Check stop locations
        for stop_location in self.stop_locations:
            if stop_location in text:
                logger.info(f"Stop location found: '{stop_location}'")
                return False
        
        return True
    
    def check_required_words(self, description: str) -> bool:
        """
        Check if listing contains at least one required word.
        
        Args:
            description: Listing description
            
        Returns:
            True if at least one required word found, False otherwise
        """
        text = description.lower()
        
        for required_word in self.required_words:
            if required_word in text:
                logger.info(f"Required word found: '{required_word}'")
                return True
        
        logger.info(f"No required words found. Need one of: {self.required_words}")
        return False
    
    def extract_phone_number(self, description: str) -> Optional[str]:
        """
        Extract Indonesian phone number from description.
        
        Args:
            description: Listing description
            
        Returns:
            Phone number if found, None otherwise
        """
        for pattern in self.phone_patterns:
            match = pattern.search(description)
            if match:
                phone = match.group(0)
                logger.info(f"Phone number found: {phone}")
                return phone
        
        return None
    
    def filter(
        self,
        title: str,
        price: str,
        description: str
    ) -> Tuple[bool, Optional[str], str]:
        """
        Apply all Level 0 filters to a listing.
        
        Args:
            title: Listing title
            price: Listing price
            description: Listing description
            
        Returns:
            Tuple of (passed: bool, phone_number: Optional[str], reason: str)
        """
        # Check price range (now with bedroom-aware pricing)
        if not self.check_price_range(price, title, description):
            return False, None, "Price out of range"

        # Hard gate: reject only explicit 1/2/3 bedrooms.
        bedrooms = self.extract_bedrooms(title, description)
        if bedrooms is not None and bedrooms < self.bedrooms_min:
            return False, None, f"Bedrooms: {bedrooms} (need {self.bedrooms_min}+)"
        
        # Check stop words
        if not self.check_stop_words(title, description):
            return False, None, "Contains stop words"
        
        # Check required words
        if not self.check_required_words(description):
            return False, None, "Missing required words (kitchen)"
        
        # Extract phone number
        phone = self.extract_phone_number(description)
        
        logger.info("Listing passed Level 0 filter")
        return True, phone, "Passed Level 0"
