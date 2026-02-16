#!/usr/bin/env python3
"""
STAGE 4: Deduplication and Russian summary generation
Groups listings by title, finds full duplicates, generates brief Russian summaries with OpenRouter.
"""

import os
import sys
import logging
import json
import time
from pathlib import Path
from dotenv import load_dotenv
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


def generate_summary_ru(listing: dict, or_client: OpenRouterClient, config: dict, last_request_time: dict) -> str:
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
        # Get OpenRouter config (rate limit only; retries are inside OpenRouterClient)
        or_cfg = (config.get("llm", {}) or {}).get("openrouter", {}) or {}
        request_delay = float(or_cfg.get("request_delay", 1.0))
        
        # Rate limiting: wait before making request
        if last_request_time.get('time'):
            elapsed = time.time() - last_request_time['time']
            if elapsed < request_delay:
                wait_time = request_delay - elapsed
                logger.debug(f"Rate limiting: waiting {wait_time:.2f}s")
                time.sleep(wait_time)
        
        title = str(listing.get('title') or 'N/A')
        price = str(listing.get('price') or 'N/A')
        location = str(listing.get('location') or 'N/A')
        description = str(listing.get('description') or 'N/A')

        # Build full description with metadata
        full_text = f"""Заголовок: {title}
Цена: {price}
Локация: {location}
Описание: {description[:800]}"""
        
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

        summary = or_client.generate_text(prompt, model=or_cfg.get("model"))

        # Update last request time
        last_request_time['time'] = time.time()
        return (summary or "").strip()
        
    except Exception as e:
        logger.error(f"OpenRouter summary error for {listing.get('fb_id')}: {e}")
        # Fallback to description snippet
        desc = str(listing.get('description') or 'Нет описания')[:150]
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
    openrouter_api_key = os.getenv('OPENROUTER_API_KEY')
    
    if not all([db_url, openrouter_api_key]):
        logger.error("Missing required environment variables (DATABASE_URL, OPENROUTER_API_KEY)!")
        sys.exit(1)
    
    # Initialize OpenRouter client (Gemini removed)
    or_client = OpenRouterClient(config, openrouter_api_key)
    logger.info("✓ OpenRouter client initialized")
    
    # Get listings with status 'stage3'
    with Database() as db:
        query = """
            SELECT fb_id, title, description, location, price, price_extracted,
                   created_at,
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
                summary_ru = generate_summary_ru(listing, or_client, config, last_request_time)
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
                # Multiple listings with same title - split into exact-content buckets.
                logger.info(f"\nChecking {len(group)} listings with title: {title[:50]}...")

                duplicate_buckets = {}
                for listing in group:
                    key = (
                        listing.get('description'),
                        listing.get('location'),
                        listing.get('price_extracted'),
                    )
                    duplicate_buckets.setdefault(key, []).append(listing)

                for bucket in duplicate_buckets.values():
                    # Keep the oldest row as canonical/original.
                    bucket_sorted = sorted(
                        bucket,
                        key=lambda x: (x.get('created_at') is None, x.get('created_at'), str(x.get('fb_id'))),
                    )
                    canonical = bucket_sorted[0]
                    canonical_fb_id = canonical['fb_id']

                    logger.info(f"  ✓ CANONICAL: {canonical_fb_id}")
                    summary_ru = generate_summary_ru(canonical, or_client, config, last_request_time)
                    logger.info(f"    Summary: {summary_ru[:100]}...")

                    db.cursor.execute(
                        "UPDATE listings SET summary_ru = %s, status = 'stage4' WHERE fb_id = %s",
                        (summary_ru, canonical_fb_id)
                    )
                    db.conn.commit()

                    unique_count += 1
                    summaries_generated += 1

                    # Mark remaining rows as duplicates of canonical.
                    for dup in bucket_sorted[1:]:
                        dup_fb_id = dup['fb_id']
                        logger.info(f"  ✗ DUPLICATE: {dup_fb_id} is duplicate of {canonical_fb_id}")
                        db.cursor.execute(
                            "UPDATE listings SET status = 'stage4_duplicate' WHERE fb_id = %s",
                            (dup_fb_id,)
                        )
                        db.conn.commit()
                        duplicates_found += 1
    
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
