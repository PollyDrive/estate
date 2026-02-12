#!/usr/bin/env python3
"""
STAGE 4: Deduplication and Russian summary generation
Groups listings by title, finds full duplicates, generates brief Russian summaries with Zhipu.
"""

import os
import sys
import logging
import json
import time
from pathlib import Path
from dotenv import load_dotenv
from google import genai
from google.genai import types
import random

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from database import Database
from llm_filters import OpenRouterClient

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/stage4.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


def generate_summary_ru(listing: dict, gemini_client: genai.Client, config: dict, last_request_time: dict) -> str:
    """
    Generate brief Russian summary using Zhipu GLM-4 with rate limiting.
    
    Args:
        listing: Listing dictionary with title, description, etc.
        zhipu_client: Zhipu client instance
        config: Configuration dictionary
        last_request_time: Dictionary tracking last request time (mutable)
        
    Returns:
        Brief Russian summary text
    """
    try:
        # Get Gemini config
        gemini_config = config['llm']['gemini']
        request_delay = gemini_config.get('request_delay', 1.0)
        retry_cfg = gemini_config.get("retry", {}) or {}
        max_retries = int(retry_cfg.get("max_retries", 6))
        base_delay = float(retry_cfg.get("base_delay", 1.0))
        max_delay = float(retry_cfg.get("max_delay", 30.0))
        jitter = float(retry_cfg.get("jitter", 0.15))

        def get_error_code(err: Exception):
            for attr in ("status_code", "code", "status", "http_status"):
                v = getattr(err, attr, None)
                if isinstance(v, int):
                    return v
                if isinstance(v, str) and v.isdigit():
                    return int(v)
            msg = str(err)
            if "429" in msg:
                return 429
            if "503" in msg:
                return 503
            return None

        def is_retryable(err: Exception) -> bool:
            return get_error_code(err) in (429, 500, 502, 503, 504)

        def list_models() -> list[str]:
            names: list[str] = []
            try:
                for m in gemini_client.models.list():
                    name = getattr(m, "name", None) or ""
                    if name:
                        names.append(name.split("/")[-1])
            except Exception as e:
                logger.warning(f"Could not list Gemini models: {e}")
            return names

        def pick_fallback_model(available: list[str], requested: str) -> str | None:
            if not available:
                return None
            req = requested.split("/")[-1]
            candidates = [
                req,
                f"{req}-latest",
                f"{req}-001",
                f"{req}-002",
                "gemini-2.0-flash-001",
                "gemini-2.5-flash",
            ]
            for cand in candidates:
                if cand in available:
                    return cand
            for cand in available:
                if "flash" in cand:
                    return cand
            return None

        def sleep_backoff(attempt: int, code):
            delay = min(max_delay, base_delay * (2 ** attempt))
            delay = delay + (random.random() * delay * jitter)
            logger.warning(f"Gemini temporary error (code={code}). Backing off {delay:.2f}s (attempt {attempt + 1}/{max_retries})")
            time.sleep(delay)
        
        # Rate limiting: wait before making request
        if last_request_time.get('time'):
            elapsed = time.time() - last_request_time['time']
            if elapsed < request_delay:
                wait_time = request_delay - elapsed
                logger.debug(f"Rate limiting: waiting {wait_time:.2f}s")
                time.sleep(wait_time)
        
        # Build full description with metadata
        full_text = f"""Заголовок: {listing.get('title', 'N/A')}
Цена: {listing.get('price', 'N/A')}
Локация: {listing.get('location', 'N/A')}
Описание: {listing.get('description', 'N/A')[:800]}"""
        
        # Prompt for structured list format (strict template)
        prompt = f"""Извлеки из объявления ключевую информацию и верни СТРОГО в формате списка с маркерами.

ФОРМАТ (используй ТОЛЬКО маркеры •):
• [количество] спальни/спален
• [район, город]
• [удобства через запятую: бассейн, кухня, AC, WiFi и т.д.]
• [цена]/мес

ПРАВИЛА:
- Каждый пункт начинается с •
- Каждый пункт на новой строке
- БЕЗ лишних слов и предложений
- Если информация отсутствует - пропускай пункт
- НЕ добавляй эмодзи
- НЕ добавляй комментарии

ПРИМЕР:
• 2 спальни
• Убуд, Бали
• Бассейн, кухня, AC, WiFi
• 12 млн IDR/мес

Текст объявления:
{full_text}

СПИСОК:"""

        model_to_use = gemini_config["model"]
        response = None
        last_err = None
        for attempt in range(max_retries + 1):
            try:
                response = gemini_client.models.generate_content(
                    model=model_to_use,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        temperature=0.1,
                        max_output_tokens=150,
                    ),
                )
                break
            except Exception as e:
                last_err = e
                code = get_error_code(e)
                if code == 404:
                    available = list_models()
                    fallback = pick_fallback_model(available, model_to_use)
                    if fallback and fallback != model_to_use and attempt < max_retries:
                        logger.warning(f"Gemini model '{model_to_use}' not found; falling back to '{fallback}'")
                        model_to_use = fallback
                        continue
                if not is_retryable(e) or attempt >= max_retries:
                    raise
                sleep_backoff(attempt, code)
        if response is None and last_err:
            raise last_err
        
        # Update last request time
        last_request_time['time'] = time.time()
        
        summary = (getattr(response, "text", None) or "").strip()
        return summary
        
    except Exception as e:
        logger.error(f"Gemini summary error for {listing.get('fb_id')}: {e}")
        # Fallback to OpenRouter if configured
        openrouter_key = os.getenv("OPENROUTER_API_KEY")
        or_cfg = config.get("llm", {}).get("openrouter", {}) or {}
        if openrouter_key and or_cfg.get("enabled", True):
            try:
                or_client = OpenRouterClient(config, openrouter_key)
                summary = or_client.generate_text(prompt, model=or_cfg.get("model"))
                if summary:
                    return summary
            except Exception as oe:
                logger.error(f"OpenRouter summary fallback error for {listing.get('fb_id')}: {oe}")
        # Fallback to description snippet
        desc = listing.get('description', 'Нет описания')[:150]
        return f"Описание: {desc}..."


def main():
    """Run Stage 4: Deduplication and summary generation"""
    
    logger.info("=" * 80)
    logger.info("STAGE 4: Deduplication and Russian Summary Generation")
    logger.info("=" * 80)
    
    # Load environment
    load_dotenv()
    
    # Load config
    config_path = 'config/config.json'
    if not os.path.exists(config_path):
        config_path = '/app/config/config.json'
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    # Get credentials
    db_url = os.getenv('DATABASE_URL')
    gemini_api_key = os.getenv('GEMINI_API_KEY')
    
    if not all([db_url, gemini_api_key]):
        logger.error("Missing required environment variables (DATABASE_URL, GEMINI_API_KEY)!")
        sys.exit(1)
    
    # Initialize Gemini client (default SDK uses beta endpoints; allow override via config)
    api_version = config.get("llm", {}).get("gemini", {}).get("api_version") or "v1beta"
    gemini_client = genai.Client(
        api_key=gemini_api_key,
        http_options=types.HttpOptions(api_version=api_version),
    )
    logger.info("✓ Gemini client initialized")
    
    # Get listings with status 'stage3'
    with Database() as db:
        query = """
            SELECT fb_id, title, description, location, price, price_extracted,
                   phone_number, bedrooms, listing_url, source
            FROM listings
            WHERE status = 'stage3'
            ORDER BY created_at DESC
        """
        db.cursor.execute(query)
        columns = [desc[0] for desc in db.cursor.description]
        listings = [dict(zip(columns, row)) for row in db.cursor.fetchall()]
    
    if not listings:
        logger.warning("No listings found with status 'stage3'")
        sys.exit(0)
    
    logger.info(f"Found {len(listings)} listings to process")
    
    # Group by title for deduplication
    groups = {}
    for listing in listings:
        title = listing['title'] or 'NO_TITLE'
        if title not in groups:
            groups[title] = []
        groups[title].append(listing)
    
    logger.info(f"Grouped into {len(groups)} unique titles")
    
    # Process deduplication
    duplicates_found = 0
    unique_count = 0
    summaries_generated = 0
    
    # Track last request time for rate limiting
    last_request_time = {'time': None}
    
    with Database() as db:
        for title, group in groups.items():
            if len(group) == 1:
                # Single listing - no duplicates
                listing = group[0]
                fb_id = listing['fb_id']
                
                logger.info(f"\nProcessing unique: {fb_id}")
                
                # Generate Russian summary
                summary_ru = generate_summary_ru(listing, gemini_client, config, last_request_time)
                logger.info(f"  Summary: {summary_ru[:100]}...")
                
                # Update with summary and status
                db.cursor.execute(
                    "UPDATE listings SET summary_ru = %s, status = 'stage4' WHERE fb_id = %s",
                    (summary_ru, fb_id)
                )
                db.conn.commit()
                
                unique_count += 1
                summaries_generated += 1
                
            else:
                # Multiple listings with same title - check for full duplicates
                logger.info(f"\nChecking {len(group)} listings with title: {title[:50]}...")
                
                # Compare each pair
                processed_in_group = set()
                
                for i, listing1 in enumerate(group):
                    fb_id1 = listing1['fb_id']
                    
                    if fb_id1 in processed_in_group:
                        continue
                    
                    is_duplicate = False
                    
                    for j, listing2 in enumerate(group[i+1:], start=i+1):
                        fb_id2 = listing2['fb_id']
                        
                        if fb_id2 in processed_in_group:
                            continue
                        
                        # Check for FULL match: description, location, price
                        if (listing1.get('description') == listing2.get('description') and
                            listing1.get('location') == listing2.get('location') and
                            listing1.get('price_extracted') == listing2.get('price_extracted')):
                            
                            # Full duplicate found
                            logger.info(f"  ✗ DUPLICATE: {fb_id2} is duplicate of {fb_id1}")
                            
                            db.cursor.execute(
                                "UPDATE listings SET status = 'stage4_duplicate' WHERE fb_id = %s",
                                (fb_id2,)
                            )
                            db.conn.commit()
                            
                            processed_in_group.add(fb_id2)
                            duplicates_found += 1
                            is_duplicate = True
                    
                    # If this is not a duplicate, generate summary
                    if not is_duplicate and fb_id1 not in processed_in_group:
                        logger.info(f"  ✓ UNIQUE: {fb_id1}")
                        
                        # Generate Russian summary
                        summary_ru = generate_summary_ru(listing1, gemini_client, config, last_request_time)
                        logger.info(f"    Summary: {summary_ru[:100]}...")
                        
                        # Update with summary and status
                        db.cursor.execute(
                            "UPDATE listings SET summary_ru = %s, status = 'stage4' WHERE fb_id = %s",
                            (summary_ru, fb_id1)
                        )
                        db.conn.commit()
                        
                        processed_in_group.add(fb_id1)
                        unique_count += 1
                        summaries_generated += 1
    
    # Summary
    logger.info("=" * 80)
    logger.info("STAGE 4 COMPLETE")
    logger.info(f"Total processed: {len(listings)}")
    logger.info(f"Unique listings: {unique_count}")
    logger.info(f"Duplicates found: {duplicates_found}")
    logger.info(f"Russian summaries generated: {summaries_generated}")
    logger.info("")
    logger.info("Listings with status 'stage4' are ready for Telegram")
    logger.info("Command: python3 scripts/send_to_telegram.py")
    logger.info("=" * 80)


if __name__ == '__main__':
    main()
