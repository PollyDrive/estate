"""
Facebook Marketplace scraper using memo23/facebook-marketplace-cheerio actor.
More advanced than standard Apify Marketplace scraper with monitoring mode support.
"""

import logging
import time
from typing import List, Dict, Optional
from apify_client import ApifyClient

logger = logging.getLogger(__name__)


class FacebookMarketplaceCheerioScraper:
    """Scraper using memo23/facebook-marketplace-cheerio Apify actor."""
    
    # Actor ID from Apify Store
    ACTOR_ID = "memo23/facebook-marketplace-cheerio"  # Back to original
    
    def __init__(self, api_key: str, config: Dict):
        """
        Initialize Cheerio Marketplace scraper.
        
        Args:
            api_key: Apify API key
            config: Configuration dict with 'marketplace_cheerio' section
        """
        self.client = ApifyClient(api_key)
        self.config = config
        self.cheerio_config = config.get('marketplace_cheerio', {})
        
        logger.info(f"FacebookMarketplaceCheerioScraper initialized")
    
    def scrape_listings(self) -> List[Dict]:
        """
        Scrape Facebook Marketplace listings using Cheerio actor.
        
        Returns:
            List of normalized listing dicts
        """
        # Prepare actor input
        actor_input = self._build_actor_input()
        
        logger.info(f"Starting actor {self.ACTOR_ID}")
        logger.info(f"Actor input: {actor_input}")
        
        try:
            # Run the actor
            run = self.client.actor(self.ACTOR_ID).call(run_input=actor_input)
            
            logger.info(f"Actor run ID: {run['id']}")
            logger.info(f"Actor status: {run['status']}")
            
            # Fetch results from dataset
            dataset_id = run['defaultDatasetId']
            items = list(self.client.dataset(dataset_id).iterate_items())
            
            logger.info(f"Fetched {len(items)} items from dataset")
            
            # Normalize listings
            normalized = []
            for item in items:
                listing = self.normalize_listing(item)
                if listing:
                    normalized.append(listing)
            
            logger.info(f"Normalized {len(normalized)} listings")
            return normalized
            
        except Exception as e:
            logger.error(f"Error running actor: {e}")
            raise
    
    def scrape_titles_only(self, max_items: int = 100) -> List[Dict]:
        """
        STAGE 1: Scrape only titles, price, location (no seller info, no full description).
        This is cheap and fast for initial filtering.
        
        Args:
            max_items: Maximum number of items to scrape
            
        Returns:
            List of normalized listing dicts with title-level data only
        """
        # Build custom actor input with includeSeller=False
        actor_input = self._build_actor_input()
        actor_input['includeSeller'] = False  # Don't fetch seller info or full description
        actor_input['maxItems'] = max_items
        
        logger.info(f"[STAGE 1] Starting title-only scrape (includeSeller=False, maxItems={max_items})")
        logger.info(f"Actor input: {actor_input}")
        
        try:
            # Run the actor
            run = self.client.actor(self.ACTOR_ID).call(run_input=actor_input)
            
            logger.info(f"Actor run ID: {run['id']}")
            logger.info(f"Actor status: {run['status']}")
            
            # Fetch results from dataset
            dataset_id = run['defaultDatasetId']
            items = list(self.client.dataset(dataset_id).iterate_items())
            
            logger.info(f"[STAGE 1] Fetched {len(items)} items (title-only)")
            
            # Normalize listings
            normalized = []
            logger.info(f"[DEBUG] Starting normalization of {len(items)} items")
            for i, item in enumerate(items):
                import json
                logger.info(f"[DEBUG] Item {i+1} keys: {list(item.keys())}")
                logger.info(f"[DEBUG] Item {i+1} raw data: {json.dumps(item, indent=2)[:500]}")
                listing = self.normalize_listing(item)
                logger.info(f"[DEBUG] Item {i+1} normalized: {listing is not None}")
                if listing:
                    normalized.append(listing)
            
            logger.info(f"[STAGE 1] Normalized {len(normalized)} title-only listings")
            return normalized
            
        except Exception as e:
            logger.error(f"[STAGE 1] Error running actor: {e}")
            raise
    
    def scrape_full_details(self, listing_urls: List[str], max_stage2_items: int = 50) -> List[Dict]:
        """
        STAGE 2: Scrape full details (including seller info) for specific URLs.
        This is expensive but only runs on filtered candidates.
        
        Args:
            listing_urls: List of Facebook Marketplace URLs to scrape
            max_stage2_items: Safety limit for Stage 2 (default: 50)
            
        Returns:
            List of normalized listing dicts with full details
        """
        if not listing_urls:
            logger.warning("[STAGE 2] No URLs provided for full detail scraping")
            return []
        
        # Safety check: limit Stage 2 items
        if len(listing_urls) > max_stage2_items:
            logger.warning(f"[STAGE 2] Limiting from {len(listing_urls)} to {max_stage2_items} items (safety limit)")
            listing_urls = listing_urls[:max_stage2_items]
        
        # Build custom actor input with specific URLs
        start_urls = [{"url": url} for url in listing_urls]
        
        actor_input = {
            "startUrls": start_urls,
            "includeSeller": True,  # Fetch full details including seller
            "monitoringMode": False,  # Not monitoring, just fetching specific URLs
            "maxItems": len(listing_urls),
            "minDelay": self.cheerio_config.get('min_delay', 5),
            "maxDelay": self.cheerio_config.get('max_delay', 10),
            "maxConcurrency": self.cheerio_config.get('max_concurrency', 10),
            "minConcurrency": self.cheerio_config.get('min_concurrency', 1),
            "maxRequestRetries": self.cheerio_config.get('max_request_retries', 100),
        }
        
        # Add proxy configuration
        proxy_config = self.cheerio_config.get('proxy')
        if proxy_config:
            actor_input["proxyConfiguration"] = proxy_config
        else:
            actor_input["proxyConfiguration"] = {
                "useApifyProxy": True,
                "apifyProxyGroups": ["RESIDENTIAL"]
            }
        
        logger.info(f"[STAGE 2] Starting full detail scrape for {len(listing_urls)} URLs")
        logger.info(f"Actor input: {actor_input}")
        
        try:
            # Run the actor
            run = self.client.actor(self.ACTOR_ID).call(run_input=actor_input)
            
            logger.info(f"Actor run ID: {run['id']}")
            logger.info(f"Actor status: {run['status']}")
            
            # Fetch results from dataset
            dataset_id = run['defaultDatasetId']
            items = list(self.client.dataset(dataset_id).iterate_items())
            
            logger.info(f"[STAGE 2] Fetched {len(items)} items with full details")
            
            # Create URL mapping for adding missing data
            # Items are returned in order, but some URLs may have failed
            # We need to match items with URLs based on title similarity or order
            url_id_map = {}
            for url in listing_urls:
                # Extract ID from URL: .../item/1234567890
                if '/item/' in url:
                    fb_id = url.split('/item/')[-1].split('?')[0].split('/')[0]
                    url_id_map[url] = fb_id
            
            logger.info(f"[DEBUG] URL to ID mapping: {len(url_id_map)} URLs")
            
            # Normalize listings and inject URL/ID
            normalized = []
            url_index = 0  # Track which URL we're processing
            
            for i, item in enumerate(items):
                # Try to find matching URL for this item
                # Since actor doesn't return URL, we match by order (not perfect but workable)
                if url_index < len(listing_urls):
                    injected_url = listing_urls[url_index]
                    injected_id = url_id_map.get(injected_url)
                    
                    # Inject URL and ID into item before normalization
                    item['_injected_url'] = injected_url
                    item['_injected_id'] = injected_id
                    
                    logger.debug(f"[STAGE 2] Item {i+1}: injecting URL={injected_url}, ID={injected_id}")
                    url_index += 1
                
                listing = self.normalize_listing(item)
                if listing:
                    normalized.append(listing)
            
            logger.info(f"[STAGE 2] Normalized {len(normalized)} full-detail listings")
            return normalized
            
        except Exception as e:
            logger.error(f"[STAGE 2] Error running actor: {e}")
            raise
    
    def _build_actor_input(self) -> Dict:
        """
        Build input configuration for the actor.
        
        Returns:
            Actor input dict
        """
        # Get URLs from config
        start_urls = []
        for url in self.cheerio_config.get('marketplace_urls', []):
            start_urls.append({"url": url})
        
        if not start_urls:
            raise ValueError("No marketplace URLs configured in 'marketplace_cheerio.marketplace_urls'")
        
        # Build actor input with all parameters
        actor_input = {
            "startUrls": start_urls,
            "includeSeller": self.cheerio_config.get('include_seller', True),
            "monitoringMode": self.cheerio_config.get('monitoring_mode', True),
            "maxItems": self.cheerio_config.get('max_items', 100),
            "minDelay": self.cheerio_config.get('min_delay', 5),
            "maxDelay": self.cheerio_config.get('max_delay', 10),
            "maxConcurrency": self.cheerio_config.get('max_concurrency', 10),
            "minConcurrency": self.cheerio_config.get('min_concurrency', 1),
            "maxRequestRetries": self.cheerio_config.get('max_request_retries', 100),
        }
        
        # Add cookies if specified
        cookies_file = self.cheerio_config.get('cookies_file')
        if cookies_file:
            try:
                import os
                if os.path.exists(cookies_file):
                    with open(cookies_file, 'r') as f:
                        cookies_raw = f.read()
                    actor_input["cookies"] = cookies_raw
                    logger.info(f"Added cookies from {cookies_file}")
                else:
                    logger.warning(f"Cookies file not found: {cookies_file}")
            except Exception as e:
                logger.warning(f"Could not load cookies: {e}")
        
        # Add proxy configuration if specified
        proxy_config = self.cheerio_config.get('proxy')
        if proxy_config:
            actor_input["proxyConfiguration"] = proxy_config
        else:
            # Default to Apify residential proxy
            actor_input["proxyConfiguration"] = {
                "useApifyProxy": True,
                "apifyProxyGroups": ["RESIDENTIAL"]
            }
        
        return actor_input
    
    def normalize_listing(self, raw: Dict) -> Optional[Dict]:
        """
        Normalize raw actor output to standard listing format.
        
        Args:
            raw: Raw listing dict from actor
            
        Returns:
            Normalized listing dict or None if invalid
        """
        try:
            # Extract from moreDetails structure (new format)
            more_details = raw.get('moreDetails', {})
            
            # Log available fields for debugging
            logger.debug(f"Raw keys: {list(raw.keys())}")
            logger.debug(f"moreDetails keys: {list(more_details.keys())}")
            
            # Check for injected data (from scrape_full_details)
            injected_id = raw.get('_injected_id')
            injected_url = raw.get('_injected_url')
            
            if injected_id:
                listing_id = injected_id
                logger.info(f"Using injected ID: {listing_id}")
            else:
                # Extract listing ID from various sources
                # Priority: moreDetails.id > moreDetails.listing_id > raw.id > raw.url extraction > generated from title
                listing_id = (more_details.get('id') or 
                             more_details.get('listing_id') or 
                             raw.get('id') or 
                             raw.get('listing_id'))
                
                # Try to extract from URL if available
                if not listing_id:
                    url_to_parse = raw.get('url') or raw.get('listingUrl') or more_details.get('url', '')
                    if url_to_parse and '/item/' in url_to_parse:
                        listing_id = url_to_parse.split('/item/')[-1].split('?')[0].split('/')[0]
                        logger.info(f"Extracted ID from URL: {listing_id}")
                
                # If still no ID found, generate from title hash as last resort
                if not listing_id:
                    title_temp = more_details.get('marketplace_listing_title', '')
                    if title_temp:
                        import hashlib
                        listing_id = hashlib.md5(title_temp.encode()).hexdigest()[:12]
                        logger.warning(f"No ID found, generated from title hash: {listing_id}")
                    else:
                        logger.warning(f"Cannot extract or generate ID - no ID fields or title")
                        return None
                
                logger.info(f"Using listing ID: {listing_id}")
            
            # Extract title from various possible fields
            title = (more_details.get('marketplace_listing_title') or 
                    more_details.get('base_marketplace_listing_title') or
                    raw.get('marketplace_listing_title', '') or 
                    raw.get('custom_title', ''))
            
            # Extract description
            description = more_details.get('description', '')
            
            # Extract price from nested structure
            price_data = more_details.get('listing_price', {}) or raw.get('listing_price', {})
            price_amount = price_data.get('amount', '')
            price_formatted = price_data.get('formatted_amount_zeros_stripped', '') or price_data.get('formatted_amount', '')
            
            # Extract location
            location_text = more_details.get('location_text', '')
            location_data = more_details.get('location', {}) or raw.get('location', {})
            reverse_geocode = location_data.get('reverse_geocode', {})
            city = reverse_geocode.get('city', '') or location_text
            state = reverse_geocode.get('state', '')
            
            # Extract coordinates
            latitude = location_data.get('latitude')
            longitude = location_data.get('longitude')
            
            # Extract image
            primary_photo = raw.get('primary_listing_photo', {})
            image_url = primary_photo.get('photo_image_url')
            
            # Extract listing URL
            # Priority: injected URL > raw fields > construct from ID
            if injected_url:
                listing_url = injected_url
                logger.info(f"Using injected URL: {listing_url}")
            else:
                listing_url = (raw.get('listingUrl') or 
                              raw.get('url') or 
                              more_details.get('url') or
                              f"https://www.facebook.com/marketplace/item/{listing_id}")
            
            # Extract seller data if available
            seller_data = more_details.get('marketplace_listing_seller')
            seller_name = None
            if seller_data:
                seller_name = seller_data.get('name')
            
            # Check if sold/pending
            is_sold = raw.get('is_sold', False)
            is_pending = raw.get('is_pending', False)
            is_live = raw.get('is_live', True)
            
            # Skip if not live
            if not is_live or is_sold or is_pending:
                logger.info(f"Skipping listing {listing_id} - not live/sold/pending")
                return None
            
            # Create normalized listing
            listing = {
                'fb_id': listing_id,
                'source': 'marketplace_cheerio',
                'group_id': None,
                'title': title[:500] if title else '',
                'description': description,
                'listing_url': listing_url,
                'image_url': image_url,
                'all_images': [image_url] if image_url else [],
                'price': price_formatted,
                'price_raw': price_amount,
                'location': location_text,
                'city': city,
                'state': state,
                'latitude': latitude,
                'longitude': longitude,
                'seller_name': seller_name,
                'timestamp': None,  # Actor doesn't provide timestamp
                'raw_data': raw,  # Keep full raw data for debugging
            }
            
            return listing
            
        except Exception as e:
            import traceback
            logger.error(f"Error normalizing listing: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            logger.error(f"Raw data keys: {list(raw.keys()) if isinstance(raw, dict) else 'not a dict'}")
            return None
