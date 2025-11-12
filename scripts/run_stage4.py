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
from zhipuai import ZhipuAI

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from database import Database

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


def generate_summary_ru(listing: dict, zhipu_client: ZhipuAI, config: dict, last_request_time: dict) -> str:
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
        # Get Zhipu config
        zhipu_config = config['llm']['zhipu']
        request_delay = zhipu_config.get('request_delay', 1.0)
        
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

        response = zhipu_client.chat.completions.create(
            model=zhipu_config['model'],
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
            max_tokens=150
        )
        
        # Update last request time
        last_request_time['time'] = time.time()
        
        summary = response.choices[0].message.content.strip()
        return summary
        
    except Exception as e:
        logger.error(f"Error generating summary for {listing.get('fb_id')}: {e}")
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
    zhipu_api_key = os.getenv('ZHIPU_API_KEY')
    
    if not all([db_url, zhipu_api_key]):
        logger.error("Missing required environment variables (DATABASE_URL, ZHIPU_API_KEY)!")
        sys.exit(1)
    
    # Initialize Zhipu client
    zhipu_client = ZhipuAI(api_key=zhipu_api_key)
    logger.info("✓ Zhipu client initialized")
    
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
                summary_ru = generate_summary_ru(listing, zhipu_client, config, last_request_time)
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
                        summary_ru = generate_summary_ru(listing1, zhipu_client, config, last_request_time)
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
