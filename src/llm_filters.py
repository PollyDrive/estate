import json
import logging
import os
import time
from typing import Dict, Optional, Tuple
from anthropic import Anthropic
from zhipuai import ZhipuAI

logger = logging.getLogger(__name__)


class ZhipuFilter:
    """
    Zhipu AI filter using GLM-4 model.
    Strict categorization of listings based on rental criteria.
    """
    
    def __init__(self, config: Dict, api_key: str):
        """
        Initialize Zhipu filter with API key.
        
        Args:
            config: Configuration dictionary
            api_key: Zhipu API key
        """
        self.config = config['llm']['zhipu']
        self.client = ZhipuAI(api_key=api_key)
        self.request_delay = self.config.get('request_delay', 1.0)
        self.last_request_time = 0
    
    def filter(self, description: str) -> Tuple[bool, str]:
        """
        Check if listing passes strict rental criteria using Zhipu GLM-4.
        
        Args:
            description: Listing description
            
        Returns:
            Tuple of (passed: bool, reason: str)
        """
        try:
            # Rate limiting
            current_time = time.time()
            time_since_last = current_time - self.last_request_time
            if time_since_last < self.request_delay:
                sleep_time = self.request_delay - time_since_last
                logger.debug(f"Rate limiting: sleeping {sleep_time:.2f}s")
                time.sleep(sleep_time)
            
            # Build prompt from template
            prompt = f"""You are a very strict real estate filter. Your task is to categorize a listing.
Respond with ONE category code ONLY.

RULES (check in order):
1.  **TYPE**:
    -   Check for 'dijual', 'leasehold', 'land', 'tanah', 'office', 'kos', 'kost', 'for sale', 'sold', 'warung', 'tempat jualan', 'toko'.
    -   If found -> 'REJECT_TYPE'.

2.  **BEDROOMS** (CRITICAL - read VERY carefully):
    -   ACCEPT ONLY if description EXPLICITLY mentions exactly 2 bedrooms
    -   Valid 2BR forms: '2 bed', '2 bedroom', '2BR', '2 BR', '2kt', '2 KT', '2 kamar tidur', '2 kamar', 'dua kamar', 'two bedroom'
    -   REJECT if 1 bedroom: '1 bed', '1 bedroom', '1BR', '1 kamar', '1kt', '1 room', 'studio', 'one bedroom' -> 'REJECT_BEDROOMS'
    -   REJECT if 3+ bedrooms: '3 bed', '3BR', '3kt', '4 bed', etc. -> 'REJECT_BEDROOMS'
    -   REJECT if studio -> 'REJECT_BEDROOMS'
    -   REJECT if NO bedroom count mentioned in description -> 'REJECT_BEDROOMS'
    -   IMPORTANT: '2KT' = '2 kamar tidur' = 2 bedrooms = ACCEPT
    -   IMPORTANT: If description only lists facilities without bedroom count -> 'REJECT_BEDROOMS'

3.  **TERM** (CRITICAL - read carefully):
    -   Check if price is explicitly stated as YEARLY ONLY:
        * Look for patterns: 'X mln/year', 'X jt/tahun', 'yearly X', 'price yearly', 'per year', 'per tahun', 'tahunan', '/year', '/yr', 'upfront'
        * If YEARLY price is mentioned WITHOUT monthly option -> 'REJECT_TERM'
    -   ACCEPT if:
        * 'monthly', 'bulanan', '/month', '/mo', 'per month', 'per bulan' is mentioned
        * 'monthly or yearly' (both options available)
        * No explicit term mentioned (assume monthly by default)
    -   REJECT if:
        * 'daily', 'harian', '/day' -> 'REJECT_TERM'
        * 'minimal 6 bulan', 'minimal 1 tahun' -> 'REJECT_TERM'

4.  **FURNITURE**:
    -   Check for 'unfurnished', 'kosongan'.
    -   If found -> 'REJECT_FURNITURE'.

5.  **PRICE** (IMPORTANT - check carefully):
    -   Look for monthly price: numbers followed by 'jt', 'juta', 'million', 'm', 'mln', 'mil', 'IDR', 'Rp'
    -   If price is stated as YEARLY (e.g., '180 mln/year'), calculate monthly: yearly_price / 12
    -   If monthly price > 16,000,000 IDR -> 'REJECT_PRICE'
    -   Examples of TOO EXPENSIVE (>16jt/month): 17jt, 18m, 20 million, 25jt, 30m, 50 million, 100jt, 180 mln/year, 210 mill yearly
    -   Examples of OK (<= 16jt/month): 10jt, 12m, 14 million, 15jt, 16jt

If you find a rejection, return the FIRST rejection code you find.
If ALL rules are 'PASS', return 'PASS'.

EXAMPLES:
- Description: 'Villa 1BR in Ubud, 10jt/month' -> REJECT_BEDROOMS (1 bedroom)
- Description: 'Studio apartment with kitchen' -> REJECT_BEDROOMS (studio)
- Description: '2 KT 1 Kamar Mandi - Rumah, 12jt/month' -> PASS (2KT = 2 bedrooms!)
- Description: '3 KT 2 KM - Rumah, 15jt/month' -> REJECT_BEDROOMS (3 bedrooms)
- Description: 'Land for rent, 5jt/month' -> REJECT_TYPE
- Description: 'Menerima kos perempuan' -> REJECT_TYPE (kos)
- Description: 'Di sewa tempat jualan' -> REJECT_TYPE (tempat jualan)
- Description: 'Facilities: AC, TV, kitchen' -> REJECT_BEDROOMS (no bedroom count)
- Description: '1 building 1 room with AC' -> REJECT_BEDROOMS (1 room)
- Description: 'Villa 2BR, 150jt/year' -> REJECT_TERM (yearly only)
- Description: '2BR Villa, 180 mln/year' -> REJECT_TERM (yearly only)
- Description: '2 bedroom house, monthly rent, 15jt' -> PASS
- Description: '2BR Villa, beautiful, 500jt' -> REJECT_PRICE (>16jt)
- Description: 'Two bedroom villa with pool, 14jt/month' -> PASS

Description:
{description}

CATEGORY:"""
            
            response = self.client.chat.completions.create(
                model=self.config['model'],
                messages=[{"role": "user", "content": prompt}],
                temperature=self.config['temperature'],
                max_tokens=self.config['max_tokens']
            )
            
            self.last_request_time = time.time()
            
            answer = response.choices[0].message.content.strip()
            
            # Parse response
            if answer == 'PASS':
                logger.info("Zhipu filter: PASS")
                return True, "Passed all rules"
            elif answer.startswith('REJECT_'):
                logger.info(f"Zhipu filter: {answer}")
                return False, answer
            else:
                # Unexpected format
                logger.warning(f"Unexpected Zhipu response: {answer}")
                return False, f"Unexpected response: {answer}"
                
        except Exception as e:
            logger.error(f"Zhipu API error: {e}")
            # In case of error, pass to avoid false negatives
            return True, f"Zhipu error (passed): {str(e)}"


class Level2Filter:
    """
    Level 2 filter: Paid LLM analysis using Claude 3 Haiku.
    Generates a brief summary in Russian.
    """
    
    def __init__(self, config: Dict, api_key: str):
        """
        Initialize Level 2 filter with Anthropic API.
        
        Args:
            config: Configuration dictionary
            api_key: Anthropic API key
        """
        self.config = config['llm']['claude']
        self.client = Anthropic(api_key=api_key)
    
    def filter(
        self,
        title: str,
        price: str,
        description: str
    ) -> Tuple[bool, Optional[Dict], str]:
        """
        Analyze listing and generate summary.
        
        Args:
            title: Listing title
            price: Listing price
            description: Listing description
            
        Returns:
            Tuple of (passed: bool, response_data: Optional[Dict], reason: str)
            response_data contains: summary_ru
        """
        try:
            prompt = self.config['prompt_template'].format(
                criteria=self.config['search_criteria'],
                title=title,
                price=price,
                description=description
            )
            
            message = self.client.messages.create(
                model=self.config['model'],
                max_tokens=self.config['max_tokens'],
                temperature=self.config['temperature'],
                messages=[{"role": "user", "content": prompt}]
            )
            
            response_text = message.content[0].text
            
            # Try to parse JSON from response
            try:
                # Extract JSON from response (Claude might add extra text)
                json_start = response_text.find('{')
                json_end = response_text.rfind('}') + 1
                
                if json_start >= 0 and json_end > json_start:
                    json_str = response_text[json_start:json_end]
                    response_data = json.loads(json_str)
                    
                    # Validate required field
                    if 'summary_ru' in response_data:
                        logger.info("Level 2 filter: Analysis completed by Claude")
                        return True, response_data, "Analysis completed"
                    else:
                        logger.error(f"Missing summary_ru in Claude response: {response_data}")
                        return False, None, "Invalid response format"
                else:
                    logger.error(f"No JSON found in Claude response: {response_text}")
                    return False, None, "No JSON in response"
                    
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse Claude JSON response: {e}")
                logger.error(f"Response text: {response_text}")
                return False, None, "JSON parse error"
                
        except Exception as e:
            logger.error(f"Claude API error: {e}")
            return False, None, f"Claude error: {str(e)}"
