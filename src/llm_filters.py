import json
import logging
import os
import random
import time
from typing import Dict, Optional, Tuple
from anthropic import Anthropic
import requests

logger = logging.getLogger(__name__)


class LLMProcessingError(RuntimeError):
    """Raised when LLM processing fails and no fallback succeeds."""


class OpenRouterClient:
    """
    Minimal OpenRouter Chat Completions client with exponential backoff.
    """

    def __init__(self, config: Dict, api_key: str):
        self.config = config.get("llm", {}).get("openrouter", {}) or {}
        self.api_key = api_key
        self.base_url = self.config.get("base_url", "https://openrouter.ai/api/v1").rstrip("/")
        # self.model = self.config.get("model", "openrouter/auto")
        self.model = self.config.get("model", "google/gemini-2.0-flash-exp:free")
        self.fallback_models = list(self.config.get("fallback_models", []) or [])
        self.connect_timeout = float(self.config.get("connect_timeout", 30))
        self.read_timeout = float(self.config.get("read_timeout", 180))

        retry_cfg = self.config.get("retry", {}) or {}
        self.max_retries = int(retry_cfg.get("max_retries", 6))
        self.base_delay = float(retry_cfg.get("base_delay", 1.0))
        self.max_delay = float(retry_cfg.get("max_delay", 30.0))
        self.jitter = float(retry_cfg.get("jitter", 0.15))

    def _sleep_backoff(self, attempt: int, reason: str) -> None:
        delay = min(self.max_delay, self.base_delay * (2 ** attempt))
        delay = delay + (random.random() * delay * self.jitter)
        logger.warning(f"OpenRouter temporary error ({reason}). Backing off {delay:.2f}s (attempt {attempt + 1}/{self.max_retries})")
        time.sleep(delay)

    def _chat(self, model: str, prompt: str, temperature: float, max_tokens: int) -> str:
        url = f"{self.base_url}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        r = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=(self.connect_timeout, self.read_timeout),
        )
        if r.status_code >= 400:
            try:
                detail = r.json()
            except Exception:
                detail = r.text
            raise RuntimeError(f"OpenRouter HTTP {r.status_code}: {detail}")
        data = r.json()
        return (data.get("choices", [{}])[0].get("message", {}) or {}).get("content", "") or ""

    def generate_text(self, prompt: str, *, model: Optional[str] = None) -> str:
        primary = model or self.model
        temperature = float(self.config.get("temperature", 0.1))
        max_tokens = int(self.config.get("max_tokens", 50))

        last_err: Optional[Exception] = None
        models_to_try = [primary] + [m for m in self.fallback_models if m and m != primary]

        for mi, model_to_use in enumerate(models_to_try, start=1):
            for attempt in range(self.max_retries + 1):
                try:
                    return self._chat(model_to_use, prompt, temperature, max_tokens).strip()
                except Exception as e:
                    last_err = e
                    msg = str(e)

                    # Model not found -> try next model in chain
                    if "HTTP 404" in msg:
                        logger.warning(f"OpenRouter model not found: '{model_to_use}' (try {mi}/{len(models_to_try)})")
                        break

                    # Retry on common transient failures / rate limits / network SSL issues.
                    msg_lower = msg.lower()
                    is_transient = (
                        ("HTTP 429" in msg)
                        or ("HTTP 503" in msg)
                        or ("HTTP 502" in msg)
                        or ("HTTP 504" in msg)
                        or ("timeout" in msg_lower)
                        or ("ssl" in msg_lower)
                        or ("ssleoferror" in msg_lower)
                        or ("unexpected eof" in msg_lower)
                        or ("max retries exceeded" in msg_lower)
                        or ("connection aborted" in msg_lower)
                        or ("connection reset" in msg_lower)
                    )
                    if is_transient:
                        if attempt >= self.max_retries:
                            logger.warning(f"OpenRouter retries exhausted for model '{model_to_use}' (try {mi}/{len(models_to_try)}). Switching model.")
                            break
                        self._sleep_backoff(attempt, msg.splitlines()[0][:120])
                        continue

                    # Other errors: don't loop forever; switch model once.
                    logger.warning(f"OpenRouter error for model '{model_to_use}' (try {mi}/{len(models_to_try)}): {msg.splitlines()[0][:160]}")
                    break

        if last_err:
            raise last_err
        return ""


class OpenRouterFilter:
    """
    OpenRouter-based LLM filter.
    Strict categorization of listings based on rental criteria.
    """
    
    def __init__(self, config: Dict, api_key: str):
        """
        Initialize OpenRouter filter with API key.
        
        Args:
            config: Configuration dictionary
            api_key: OpenRouter API key
        """
        self.root_config = config
        self.client = OpenRouterClient(config, api_key)
    
    def filter(self, description: str) -> Tuple[bool, str]:
        """
        Check if listing passes strict rental criteria using OpenRouter.
        
        Args:
            description: Listing description
            
        Returns:
            Tuple of (passed: bool, reason: str)
        """
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
    -   ACCEPT ONLY if description mentions 4 or more bedrooms
    -   Valid forms: '4 bed', '4BR', '4kt', '5 bed', '5BR', '5kt', etc.
    -   REJECT if 1, 2, or 3 bedrooms: '1 bed', '2BR', '3kt', '3 kamar', '3 room', 'one/two/three bedroom' -> 'REJECT_BEDROOMS'
    -   REJECT if studio -> 'REJECT_BEDROOMS'
    -   REJECT if NO bedroom count mentioned in description -> 'REJECT_BEDROOMS'
    -   IMPORTANT: '2KT' and '3KT' must be REJECT_BEDROOMS
    -   IMPORTANT: '4KT' = 4 bedrooms = ACCEPT
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
- Description: '2 KT 1 Kamar Mandi - Rumah, 12jt/month' -> REJECT_BEDROOMS
- Description: '3 KT 2 KM - Rumah, 15jt/month' -> REJECT_BEDROOMS
- Description: '4BR villa Cemagi, 12jt/month' -> PASS (4 bedrooms, OK!)
- Description: '5BR villa, 14jt/month' -> PASS (5 bedrooms, acceptable)
- Description: 'Land for rent, 5jt/month' -> REJECT_TYPE (land)
- Description: 'Menerima kos perempuan' -> REJECT_TYPE (kos)
- Description: 'Di sewa tempat jualan' -> REJECT_TYPE (tempat jualan)
- Description: '4BR villa under construction, 80% finished, 12jt' -> REJECT_TYPE (under construction)
- Description: 'New house masih dibangun, available next month' -> REJECT_TYPE (sedang dibangun)
- Description: '3BR villa, 95% done, on finishing stage, 15jt' -> REJECT_TYPE (finishing stage)
- Description: '2BR house leasehold for sale, 500jt' -> REJECT_TYPE (leasehold sale)
- Description: '3BR villa, leasehold 20 years, 10jt/month' -> REJECT_BEDROOMS
- Description: '2BR, construction nearby, 12jt/month' -> PASS (nearby construction OK)
- Description: 'Room for rent in shared house, 3jt/month' -> REJECT_ROOM_ONLY (single room)
- Description: 'Private room available in villa, 4jt' -> REJECT_ROOM_ONLY (room only)
- Description: 'Sewa kamar di rumah, AC, 5jt/bulan' -> REJECT_ROOM_ONLY (kamar saja)
- Description: 'Facilities: AC, TV, kitchen' -> REJECT_BEDROOMS (no bedroom count)
- Description: '1 building 1 room with AC' -> REJECT_BEDROOMS (1 room)
- Description: 'Villa 2BR, 150jt/year' -> REJECT_BEDROOMS
- Description: '2BR Villa, 180 mln/year' -> REJECT_BEDROOMS
- Description: '3 kamar tidur, hrg 12jt/th nett' -> REJECT_BEDROOMS
- Description: '2BR house, 100mln/6 months upfront' -> REJECT_BEDROOMS
- Description: '3BR Villa, unfurnished, 14jt/month' -> REJECT_BEDROOMS
- Description: '2BR, minimal 1 tahun, 15jt/month' -> REJECT_BEDROOMS
- Description: '2 bedroom house, daily rent, 500k/day' -> REJECT_TERM (daily rental)
- Description: 'Villa nightly rental, 2jt/night' -> REJECT_TERM (nightly rental)
- Description: '2 bedroom house, monthly rent, 15jt' -> REJECT_BEDROOMS
- Description: '2BR Villa, beautiful, 500jt' -> REJECT_BEDROOMS
- Description: 'Two bedroom villa with pool, 14jt/month' -> REJECT_BEDROOMS

Description:
{description}

CATEGORY:"""

        try:
            answer = self.client.generate_text(prompt)
        except Exception as e:
            raise LLMProcessingError(f"OpenRouter failed: {e}") from e

        answer = (answer or "").strip()
        import re

        m = re.search(r"\b(PASS|REJECT_[A-Z_]+)\b", answer)
        code = m.group(1) if m else (answer.splitlines()[0].strip() if answer else "")
        if not code:
            raise LLMProcessingError("Empty OpenRouter response")

        if code == "PASS":
            logger.info("OpenRouter filter: PASS")
            return True, "Passed all rules"
        if code.startswith("REJECT_"):
            logger.info(f"OpenRouter filter: {code}")
            return False, code

        raise LLMProcessingError(f"Unexpected OpenRouter response: {answer}")


# Backwards-compatible alias (avoid breaking older imports)
ZhipuFilter = OpenRouterFilter


def get_llm_filters(config: Dict):
    """
    Returns:
        (level1_filter, level2_filter)
    """
    openrouter_key = os.getenv("OPENROUTER_API_KEY")
    if openrouter_key and config.get("llm", {}).get("openrouter"):
        return OpenRouterFilter(config, openrouter_key), None

    return None, None


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
