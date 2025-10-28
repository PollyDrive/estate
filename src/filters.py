import re
import logging
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class Level0Filter:
    """
    Level 0 filter: Hard filters using regex and keyword matching.
    Filters by price range, stop words, required words, and extracts phone numbers.
    """
    
    def __init__(self, config: Dict):
        """
        Initialize Level 0 filter with configuration.
        
        Args:
            config: Configuration dictionary with filter settings
        """
        self.min_price = config['filters']['price']['min']
        self.max_price = config['filters']['price']['max']
        self.stop_words = [word.lower() for word in config['filters']['stop_words']]
        self.required_words = [word.lower() for word in config['filters']['required_words']]
        self.phone_patterns = [re.compile(pattern) for pattern in config['filters']['phone_regex']]
    
    def extract_price(self, price_str: str) -> Optional[int]:
        """
        Extract numeric price from price string.
        
        Args:
            price_str: Price string (e.g., "Rp 5,000,000")
            
        Returns:
            Price as integer or None if extraction fails
        """
        if not price_str:
            return None
        
        # Remove common currency symbols and text
        cleaned = re.sub(r'[Rp\s,.]', '', price_str)
        
        try:
            return int(cleaned)
        except ValueError:
            logger.warning(f"Could not extract price from: {price_str}")
            return None
    
    def check_price_range(self, price_str: str) -> bool:
        """
        Check if price is within acceptable range.
        
        Args:
            price_str: Price string
            
        Returns:
            True if price is in range, False otherwise
        """
        price = self.extract_price(price_str)
        
        if price is None:
            logger.warning(f"Price extraction failed for: {price_str}")
            return False
        
        in_range = self.min_price <= price <= self.max_price
        
        if not in_range:
            logger.info(f"Price {price} out of range ({self.min_price}-{self.max_price})")
        
        return in_range
    
    def check_stop_words(self, title: str, description: str) -> bool:
        """
        Check if listing contains any stop words.
        
        Args:
            title: Listing title
            description: Listing description
            
        Returns:
            True if NO stop words found (pass), False if stop words found (fail)
        """
        text = f"{title} {description}".lower()
        
        for stop_word in self.stop_words:
            if stop_word in text:
                logger.info(f"Stop word found: '{stop_word}'")
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
        # Check price range
        if not self.check_price_range(price):
            return False, None, "Price out of range"
        
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
