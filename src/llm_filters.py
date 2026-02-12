import json
import logging
import os
import random
import time
from typing import Dict, Optional, Tuple
from anthropic import Anthropic
from google import genai
from google.genai import types
import requests

logger = logging.getLogger(__name__)


class OpenRouterClient:
    """
    Minimal OpenRouter Chat Completions client with exponential backoff.
    """

    def __init__(self, config: Dict, api_key: str):
        self.config = config.get("llm", {}).get("openrouter", {}) or {}
        self.api_key = api_key
        self.base_url = self.config.get("base_url", "https://openrouter.ai/api/v1").rstrip("/")
        self.model = self.config.get("model", "openrouter/auto")
        self.fallback_models = list(self.config.get("fallback_models", []) or [])

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
        r = requests.post(url, headers=headers, json=payload, timeout=60)
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

                    # Retry on common transient failures / rate limits.
                    if ("HTTP 429" in msg) or ("HTTP 503" in msg) or ("HTTP 502" in msg) or ("timeout" in msg.lower()):
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


class GeminiFilter:
    """
    Gemini AI filter using Google GenAI SDK.
    Strict categorization of listings based on rental criteria.
    """
    
    def __init__(self, config: Dict, api_key: str):
        """
        Initialize Gemini filter with API key.
        
        Args:
            config: Configuration dictionary
            api_key: Gemini API key
        """
        self.root_config = config
        self.config = config['llm']['gemini']
        api_version = self.config.get("api_version") or "v1beta"
        self.client = genai.Client(
            api_key=api_key,
            http_options=types.HttpOptions(api_version=api_version),
        )
        self.model = self.config.get("model")
        self.request_delay = self.config.get('request_delay', 1.0)
        self.last_request_time = 0

        retry_cfg = self.config.get("retry", {}) or {}
        self.max_retries = int(retry_cfg.get("max_retries", 6))
        self.base_delay = float(retry_cfg.get("base_delay", 1.0))
        self.max_delay = float(retry_cfg.get("max_delay", 30.0))
        self.jitter = float(retry_cfg.get("jitter", 0.15))

    def _resolve_model_name(self) -> str:
        """
        Resolve a model id to something supported by the current API endpoint.
        If exact model isn't available, attempt a safe fallback based on ListModels.
        """
        requested = self.model
        if not requested:
            return "gemini-2.5-flash"

        # Fast path: try requested first
        return requested

    def _list_model_names(self) -> list[str]:
        names: list[str] = []
        try:
            for m in self.client.models.list():
                name = getattr(m, "name", None) or ""
                if name:
                    # Typical form: "models/gemini-2.5-flash"
                    names.append(name.split("/")[-1])
        except Exception as e:
            logger.warning(f"Could not list Gemini models: {e}")
        return names

    def _pick_fallback_model(self, available: list[str], requested: str) -> Optional[str]:
        """
        Pick the best available model name given a requested one.
        """
        if not available:
            return None

        req = requested.split("/")[-1]
        candidates = [
            req,
            f"{req}-latest",
            f"{req}-001",
            f"{req}-002",
            # Common stable flash models that tend to exist
            "gemini-2.0-flash-001",
            "gemini-2.5-flash",
        ]
        for cand in candidates:
            if cand in available:
                return cand

        # Last resort: pick any flash-ish model.
        for cand in available:
            if "flash" in cand:
                return cand

        return None

    def _get_error_code(self, err: Exception) -> Optional[int]:
        """
        Best-effort extraction of HTTP-like error code from Gemini SDK exceptions.
        """
        for attr in ("status_code", "code", "status", "http_status"):
            val = getattr(err, attr, None)
            if isinstance(val, int):
                return val
            if isinstance(val, str) and val.isdigit():
                return int(val)

        # Sometimes errors are dict-ish inside args
        try:
            if err.args:
                for a in err.args:
                    if isinstance(a, dict):
                        for k in ("code", "status_code", "status"):
                            v = a.get(k)
                            if isinstance(v, int):
                                return v
                            if isinstance(v, str) and v.isdigit():
                                return int(v)
        except Exception:
            pass

        # Fallback: parse message
        msg = str(err)
        if " 429" in msg or "429" in msg:
            return 429
        if " 503" in msg or "503" in msg:
            return 503
        return None

    def _is_retryable(self, err: Exception) -> bool:
        code = self._get_error_code(err)
        return code in (429, 500, 502, 503, 504)

    def _sleep_backoff(self, attempt: int, reason: str) -> None:
        delay = min(self.max_delay, self.base_delay * (2 ** attempt))
        # jitter: add 0..(delay*jitter)
        delay = delay + (random.random() * delay * self.jitter)
        logger.warning(f"Gemini rate-limited/temporary error ({reason}). Backing off {delay:.2f}s (attempt {attempt + 1}/{self.max_retries})")
        time.sleep(delay)

    def _generate_with_backoff(self, prompt: str):
        last_err: Optional[Exception] = None
        model_to_use = self._resolve_model_name()
        for attempt in range(self.max_retries + 1):
            try:
                return self.client.models.generate_content(
                    model=model_to_use,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        temperature=self.config.get("temperature", 0.1),
                        max_output_tokens=self.config.get("max_tokens", 50),
                    ),
                )
            except Exception as e:
                last_err = e
                code = self._get_error_code(e)
                # If model is not found, try to resolve via ListModels once.
                if code == 404:
                    available = self._list_model_names()
                    fallback = self._pick_fallback_model(available, model_to_use)
                    if fallback and fallback != model_to_use:
                        logger.warning(f"Gemini model '{model_to_use}' not found; falling back to '{fallback}'")
                        model_to_use = fallback
                        # Immediately retry with the fallback model (no backoff for 404 model resolution).
                        if attempt < self.max_retries:
                            continue
                    else:
                        logger.error(
                            "Gemini model not found and no fallback available. "
                            f"requested='{model_to_use}', available_models_sample={available[:25]}"
                        )
                if not self._is_retryable(e) or attempt >= self.max_retries:
                    raise
                self._sleep_backoff(attempt, f"code={code}")

        # Should never reach here
        if last_err:
            raise last_err
    
    def filter(self, description: str) -> Tuple[bool, str]:
        """
        Check if listing passes strict rental criteria using Gemini.
        
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
            
            response = self._generate_with_backoff(prompt)
            
            self.last_request_time = time.time()
            
            answer = (getattr(response, "text", None) or "").strip()
            # Gemini can sometimes return extra text; extract first valid code.
            import re
            m = re.search(r"\b(PASS|REJECT_[A-Z_]+)\b", answer)
            code = m.group(1) if m else answer.strip().splitlines()[0].strip()
            
            # Parse response
            if code == 'PASS':
                logger.info("Gemini filter: PASS")
                return True, "Passed all rules"
            elif code.startswith('REJECT_'):
                logger.info(f"Gemini filter: {code}")
                return False, code
            else:
                # Unexpected format
                logger.warning(f"Unexpected Gemini response: {answer}")
                return False, f"Unexpected response: {answer}"
                
        except Exception as e:
            logger.error(f"Gemini API error: {e}")
            # Fallback to OpenRouter if configured
            openrouter_key = os.getenv("OPENROUTER_API_KEY")
            try:
                or_cfg = self.root_config.get("llm", {}).get("openrouter", {})
            except Exception:
                or_cfg = {}
            if openrouter_key and (or_cfg.get("enabled", True)):
                try:
                    or_client = OpenRouterClient(self.root_config, openrouter_key)
                    answer = or_client.generate_text(prompt)
                    import re
                    m = re.search(r"\b(PASS|REJECT_[A-Z_]+)\b", answer)
                    code = m.group(1) if m else (answer.strip().splitlines()[0].strip() if answer.strip() else "")
                    if code == "PASS":
                        logger.info("OpenRouter fallback: PASS")
                        return True, "Passed all rules"
                    if code.startswith("REJECT_"):
                        logger.info(f"OpenRouter fallback: {code}")
                        return False, code
                    return False, f"Unexpected OpenRouter response: {answer}"
                except Exception as oe:
                    logger.error(f"OpenRouter fallback error: {oe}")
            # In case of error, pass to avoid false negatives
            return True, f"Gemini/OpenRouter error (passed): {str(e)}"


# Backwards-compatible alias (avoid breaking older imports)
ZhipuFilter = GeminiFilter


def get_llm_filters(config: Dict):
    """
    Returns:
        (level1_filter, level2_filter)
    """
    gemini_key = os.getenv("GEMINI_API_KEY")
    if gemini_key and config.get("llm", {}).get("gemini"):
        return GeminiFilter(config, gemini_key), None

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
