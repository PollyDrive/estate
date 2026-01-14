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
    -   REJECT these sale/commercial types: 'dijual', 'for sale', 'sold', 'land', 'tanah', 'office', 'kos', 'kost', 'warung', 'tempat jualan', 'toko'.
    -   REJECT under construction: 'under construction', 'masih dibangun', 'sedang dibangun', 'finishing stage', 'belum selesai', 'not ready', 'will be ready'.
    -   ACCEPT 'construction nearby' or 'construction next door' (not under construction itself).
    -   CRITICAL: IGNORE 'leasehold' if it's describing rental contract duration (e.g., 'Leasehold 19 Years' = rental contract term = ACCEPT).
    -   ONLY reject 'leasehold' if combined with sale words: 'leasehold for sale', 'dijual leasehold', 'selling leasehold'.
    -   If REJECT reason found -> 'REJECT_TYPE'.

1.5 **ROOM_ONLY** (CRITICAL - check for single room rentals):
    -   Check if listing is for a SINGLE ROOM inside a house/villa (not a complete property)
    -   Reject patterns: 'room for rent', 'single room', 'private room', 'kamar untuk disewa', 'kamar saja', 'sewa kamar', 'room only', 'one room', '1 room available', 'spare room', 'renting a room'
    -   Accept patterns: 'villa', 'house', 'rumah', 'apartment', 'complete unit', 'entire place', 'whole house'
    -   IMPORTANT: '1 bedroom villa' = complete villa with 1 bedroom = OK for this rule (but rejected in BEDROOMS rule)
    -   IMPORTANT: 'room for rent' without mention of complete property = REJECT_ROOM_ONLY
    -   If found room only -> 'REJECT_ROOM_ONLY'

2.  **BEDROOMS** (CRITICAL - read VERY carefully):
    -   ACCEPT if description mentions 2, 3, or 4 bedrooms
    -   Valid forms: '2 bed', '2BR', '2kt', '3 bed', '3BR', '3kt', '4 bed', '4BR', '4kt'
    -   REJECT if 1 bedroom: '1 bed', '1 bedroom', '1BR', '1 kamar', '1kt', '1 room', 'studio', 'one bedroom' -> 'REJECT_BEDROOMS'
    -   REJECT if 5+ bedrooms: '5 bed', '5BR', '5kt', '6 bed', etc. -> 'REJECT_BEDROOMS'
    -   REJECT if studio -> 'REJECT_BEDROOMS'
    -   REJECT if NO bedroom count mentioned in description -> 'REJECT_BEDROOMS'
    -   IMPORTANT: '2KT' = '2 kamar tidur' = 2 bedrooms = ACCEPT
    -   IMPORTANT: '3KT' = '3 kamar tidur' = 3 bedrooms = ACCEPT
    -   IMPORTANT: If description only lists facilities without bedroom count -> 'REJECT_BEDROOMS'

3.  **TERM** (CRITICAL - read carefully):
    -   ONLY REJECT for daily/weekly/hourly rentals:
        * 'daily', 'harian', '/day', 'per day', '/hari'
        * 'nightly', 'per night', '/night'
        * 'weekly', 'mingguan', '/week', '/minggu'
        * 'hourly', 'per hour', '/jam'
        * If found -> 'REJECT_TERM'
    -   ACCEPT for ALL other rental terms including:
        * Monthly: 'monthly', 'bulanan', '/month', '/mo', '/bulan', 'per month', 'per bulan'
        * Yearly: 'yearly', 'tahunan', '/year', '/tahun', '/th', 'per year', 'per tahun' (ALL yearly formats accepted!)
        * Multi-year: '2 tahun', '3 years', '5 tahun' (prices already converted to monthly)
        * Long-term: 'minimal 6 bulan', 'minimal 1 tahun', 'minimum 2 years', 'long term', 'kontrak tahunan'
        * Mixed: 'monthly or yearly', 'bulanan atau tahunan', 'for rent'
        * Upfront: '6 months upfront', '1 year upfront' (this is OK - long-term commitment)
        * No term mentioned (assume monthly/long-term by default)
    -   IMPORTANT: ALL yearly rentals are ACCEPTED because prices are already converted to monthly
    -   IMPORTANT: '/th' = '/tahun' = per year = ACCEPT (not reject!)

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
- Description: '3 KT 2 KM - Rumah, 15jt/month' -> PASS (3KT = 3 bedrooms, OK!)
- Description: '4BR villa Cemagi, 12jt/month' -> PASS (4 bedrooms, OK!)
- Description: '5BR villa, 14jt/month' -> REJECT_BEDROOMS (5 bedrooms, too many)
- Description: 'Land for rent, 5jt/month' -> REJECT_TYPE (land)
- Description: 'Menerima kos perempuan' -> REJECT_TYPE (kos)
- Description: 'Di sewa tempat jualan' -> REJECT_TYPE (tempat jualan)
- Description: '4BR villa under construction, 80% finished, 12jt' -> REJECT_TYPE (under construction)
- Description: 'New house masih dibangun, available next month' -> REJECT_TYPE (sedang dibangun)
- Description: '3BR villa, 95% done, on finishing stage, 15jt' -> REJECT_TYPE (finishing stage)
- Description: '2BR house leasehold for sale, 500jt' -> REJECT_TYPE (leasehold sale)
- Description: '3BR villa, leasehold 20 years, 10jt/month' -> PASS (leasehold rental term, not sale)
- Description: '2BR, construction nearby, 12jt/month' -> PASS (nearby construction OK)
- Description: 'Room for rent in shared house, 3jt/month' -> REJECT_ROOM_ONLY (single room)
- Description: 'Private room available in villa, 4jt' -> REJECT_ROOM_ONLY (room only)
- Description: 'Sewa kamar di rumah, AC, 5jt/bulan' -> REJECT_ROOM_ONLY (kamar saja)
- Description: 'Facilities: AC, TV, kitchen' -> REJECT_BEDROOMS (no bedroom count)
- Description: '1 building 1 room with AC' -> REJECT_BEDROOMS (1 room)
- Description: 'Villa 2BR, 150jt/year' -> PASS (yearly OK, price converted to 12.5jt/month)
- Description: '2BR Villa, 180 mln/year' -> PASS (yearly OK, price converted to 15jt/month)
- Description: '3 kamar tidur, hrg 12jt/th nett' -> PASS (/th = per tahun = yearly = ACCEPT!)
- Description: '2BR house, 100mln/6 months upfront' -> PASS (long-term commitment = OK)
- Description: '3BR Villa, unfurnished, 14jt/month' -> PASS (furniture not a criterion)
- Description: '2BR, minimal 1 tahun, 15jt/month' -> PASS (long-term OK)
- Description: '2 bedroom house, daily rent, 500k/day' -> REJECT_TERM (daily rental)
- Description: 'Villa nightly rental, 2jt/night' -> REJECT_TERM (nightly rental)
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
