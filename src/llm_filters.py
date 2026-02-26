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
        choices = data.get("choices") or []
        if not choices:
            raise RuntimeError(f"OpenRouter empty choices: {data}")

        message = (choices[0].get("message", {}) or {})
        content = (message.get("content") or "").strip()
        if not content:
            raise RuntimeError(f"OpenRouter empty content: {data}")
        return content

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
                        or ("empty choices" in msg_lower)
                        or ("empty content" in msg_lower)
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
    
    def filter(self, description: str) -> Tuple[bool, str, str]:
        """
        Check if listing passes strict rental criteria using OpenRouter.

        Args:
            description: Listing description

        Returns:
            Tuple of (passed: bool, reason: str, model_used: str)
        """
        # Build prompt — GLOBAL filters only (type, location, room-only, term).
        # Bedroom count and price are profile-specific and handled in stage4 --chat.
        # NOTE: We do NOT pass profile stop_locations here — those are Bali areas
        # (Ubud, Seminyak, Kuta, etc.) and belong in stage4 profile filter only.
        # Stage3 LLM only rejects listings clearly outside Bali/Indonesia.
        prompt = f"""You are a strict real estate listing filter for Bali, Indonesia rental properties.
Respond with ONE category code ONLY — no explanation, no punctuation, just the code.

RULES (check in this order, return the FIRST match):

0. LOCATION & LANGUAGE (check FIRST):
   - ACCEPT only Bali, Indonesia rentals
   - REJECT ONLY if the listing is clearly in a different country or city outside Indonesia:
     * USA cities/states: New York, NY, NJ, New Jersey, California, CA, Florida, FL, Texas, TX,
       Los Angeles, Miami, Chicago, Boston, San Francisco, Newark, Brooklyn, Queens, Manhattan
     * Other countries: Thailand, Philippines, Vietnam, Malaysia, Singapore, Australia,
       and any European or Latin American locations
     * American-style terms indicating non-Indonesia listing: 'Ranch', 'basement',
       'hardwood floors', 'sqft', 'sq ft', 'zip code', 'credit check', 'background check',
       'security deposit required'
   - ALL Bali areas are VALID (Ubud, Canggu, Seminyak, Kuta, Kerobokan, Jimbaran,
     Sanur, Uluwatu, Ungasan, Nusa Dua, Lovina, Singaraja, Tabanan, Gianyar,
     Denpasar, Buleleng, Karangasem — these are all Bali, Indonesia → ACCEPT)
   - If location is empty or unknown, check language:
     * ACCEPT: English, Indonesian, Russian
     * REJECT: Spanish, Portuguese, French, German, Chinese, Arabic, etc.
   - IMPORTANT: A listing mentioning a Bali area name (even in passing) is Bali → ACCEPT
   - Code: REJECT_LOCATION

1. TYPE — reject non-residential or non-rental:
   - REJECT sale listings: 'dijual', 'for sale', 'sold', 'forsale', 'leasehold for sale', 'dijual leasehold'
   - REJECT commercial: 'land for rent', 'tanah disewakan', 'office', 'kos', 'kost', 'warung', 'toko', 'artshop', 'restoran'
   - REJECT under construction (property itself not ready): 'under construction', 'masih dibangun', 'sedang dibangun', 'finishing stage', 'belum selesai', 'not ready yet'
   - ACCEPT 'construction nearby' (not the property itself being rented)
   - ACCEPT 'leasehold' as a rental contract term (long-term lease of land/property rights)
   - ACCEPT 'tanah' if used only as a location reference (e.g. "Jln Tanah Barak"), not as "tanah disewakan"
   - IMPORTANT: Residential houses/villas are valid regardless of bedroom count (1BR, 2BR, 3BR, 4BR+ are all allowed in Stage3)
   - IMPORTANT: 'house', 'rumah', 'villa', 'townhouse', 'apartment', 'loft' are residential types; do NOT reject them as REJECT_TYPE
   - IMPORTANT: 'X beds Y baths House' or '1 bedroom house/villa' is a valid whole-property rental unless text clearly says room-only/commercial/sale
   - IMPORTANT: Do NOT infer sale from generic marketing wording like 'dipasarkan', 'marketed', 'available again'
   - Code: REJECT_TYPE

2. ROOM_ONLY — reject single-room rentals (not a complete property):
   - REJECT: 'room for rent', 'single room', 'private room', 'spare room', 'renting a room',
     'sewa kamar', 'kamar untuk disewa', 'kamar saja', '1 room available', 'terima kost', 'kosan'
   - REJECT shared facilities (guesthouse style): 'sharing pool', 'shared pool', 'sharing kitchen',
     'shared kitchen', 'dapur bersama' — UNLESS this is clearly a complex with multiple complete units
   - ACCEPT: 'villa', 'house', 'rumah', 'apartment', 'townhouse', 'loft', 'entire place', 'whole house', 'complete unit'
   - ACCEPT: '1 bedroom villa/house', '2BR house', '4 beds 4 baths house' = complete property with its own rooms = ACCEPT here
   - IMPORTANT: If listing presents one full unit with private kitchen/living/bathroom/garden/pool, treat as whole property (PASS this rule), not room-only
   - Code: REJECT_ROOM_ONLY

3. TERM — reject short-term only listings:
   - REJECT if ONLY short-term available: 'daily', 'harian', '/day', 'per day', '/hari',
     'nightly', 'per night', '/night', 'weekly', 'mingguan', '/week', 'hourly', '/jam'
   - ACCEPT if listing offers BOTH short-term and long-term (e.g. 'daily / monthly / yearly')
   - ACCEPT: monthly, yearly, long-term, or no term mentioned (default = long-term)
   - ACCEPT: '/th' = '/tahun' = yearly = OK, 'bulanan' = monthly = OK
   - Code: REJECT_TERM

If none of the above apply → PASS

EXAMPLES:
- '4BR house in New York, $2000/month' → REJECT_LOCATION
- 'Ranch 4 Bedrooms with basement, hardwood floors, sqft' → REJECT_LOCATION
- 'Apartment in Bangkok, 15000 THB/month' → REJECT_LOCATION
- 'House in Kuala Lumpur, Malaysia' → REJECT_LOCATION
- 'Habitaciones en renta, 2 recamaras, Ciudad de Mexico' → REJECT_LOCATION
- 'Land for rent 5jt/month' → REJECT_TYPE
- 'Menerima kos perempuan saja' → REJECT_TYPE
- 'Villa 80% finished, not ready yet, under construction' → REJECT_TYPE
- 'Dijual villa 3BR leasehold 500jt' → REJECT_TYPE
- 'Room for rent in shared house, 3jt/month' → REJECT_ROOM_ONLY
- 'Private room available in villa, 4jt' → REJECT_ROOM_ONLY
- 'Villa nightly rental, 2jt/night' → REJECT_TERM
- '2 bedroom house, daily only, 500k/day' → REJECT_TERM
- '1BR villa in Canggu, 5jt/month' → PASS
- '4BR villa in Pererenan, 35jt/month' → PASS
- 'Villa Ubud with pool, leasehold 20 years, 10jt/month' → PASS
- 'Rumah 3 kamar, bulanan 8jt, Seminyak' → PASS
- '2 KT 1 KM, near Kuta, 6jt/bulan' → PASS
- 'Villa Kerobokan 2BR, 15min to Canggu and Seminyak, 12jt/month' → PASS
- 'New villa in Seseh area, 2 story, private pool, 18jt/month' → PASS
- 'Villa Ubud forest view, day use / bulanan / tahunan, sharing pool' → REJECT_ROOM_ONLY
- 'Rumah Jimbaran 3 kamar, 13jt/bulan' → PASS
- 'Rumah Ungasan 3BR monthly, 10jt' → PASS
- 'Villa Sanur 4BR, monthly/yearly, private pool' → PASS

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
            logger.warning("OpenRouter filter: empty response, fallback reject")
            return False, "REJECT_TYPE (malformed/empty LLM response)", self.client.model

        if code == "PASS":
            logger.info("OpenRouter filter: PASS")
            return True, "Passed all rules", self.client.model
        if code.startswith("REJECT_"):
            logger.info(f"OpenRouter filter: {code}")
            return False, code, self.client.model

        # Fail-safe for truncated model output (e.g. "RE", "REJ", "REJECT")
        # or other malformed one-token replies: reject listing, do not crash pipeline.
        code_upper = code.upper()
        if code_upper.startswith("RE"):
            logger.warning(
                f"OpenRouter filter: malformed reject token '{code}', fallback reject"
            )
            return False, "REJECT_TYPE (malformed LLM response)", self.client.model

        logger.warning(
            f"OpenRouter filter: unexpected response '{answer[:120]}', fallback reject"
        )
        return False, "REJECT_TYPE (unexpected LLM response)", self.client.model


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
