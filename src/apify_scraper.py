import logging
import os
from typing import List, Dict, Any
from apify_client import ApifyClient
from apify_proxy import build_apify_proxy_config

logger = logging.getLogger(__name__)


class ApifyScraper:
    """Apify Facebook Marketplace scraper."""
    
    # Actor ID for Stage 1
    ACTOR_ID = "apify/facebook-marketplace-scraper"
    
    def __init__(self, api_key: str, config: Dict):
        """
        Initialize Apify scraper.
        
        Args:
            api_key: Apify API key
            config: Configuration dictionary
        """
        self.client = ApifyClient(api_key)
        self.max_listings = config['apify']['max_listings']
        self.marketplace_urls = config['apify']['marketplace_urls']
    
    def scrape_listings(self) -> List[Dict[str, Any]]:
        """
        Scrape Facebook Marketplace listings using Apify.
        
        Returns:
            List of listing dictionaries
        """
        # Check environment variable for real vs mock scraping
        USE_REAL_APIFY = os.getenv('USE_REAL_APIFY', 'false').lower() == 'true'
        
        if not USE_REAL_APIFY:
            # MOCK DATA for testing
            logger.warning("Using MOCK DATA. Set USE_REAL_APIFY=True for real scraping")
            mock_listings = [
            {
                'id': 'mock_123456',
                'title': '2 Bedroom Villa in Ubud - Long Term',
                'price': 'Rp 8,000,000/month',
                'location': 'Ubud, Bali',
                'url': 'https://facebook.com/marketplace/item/mock123456',
                'description': 'Beautiful 2-bedroom villa for long-term monthly rent in Ubud. Fully furnished with enclosed kitchen, AC in both rooms, high-speed WiFi. Utilities included. Perfect for expats. Available now. Contact: 081234567890'
            },
            {
                'id': 'mock_789012',
                'title': 'Daily Rental Studio with Outdoor Kitchen',
                'price': 'Rp 500,000/day',
                'location': 'Seminyak, Bali',
                'url': 'https://facebook.com/marketplace/item/mock789012',
                'description': 'Short term daily rental. Outdoor kitchen only. Hotel-style studio apartment. WiFi available.'
            },
            {
                'id': 'mock_345678',
                'title': '1BR House Abiansemal - Monthly',
                'price': 'IDR 3,500,000 per month',
                'location': 'Abiansemal, Bali',
                'url': 'https://facebook.com/marketplace/item/mock345678',
                'description': 'Cozy 1-bedroom house for monthly rent in Abiansemal. Has separate closed kitchen with full appliances. AC, WiFi included. Bills included. Semi-furnished. Quiet area. WhatsApp 082345678901'
            },
            {
                'id': 'mock_999888',
                'title': '2BR Villa Singakerta with Indoor Kitchen',
                'price': 'Rp 10,000,000/month',
                'location': 'Singakerta, Ubud',
                'url': 'https://facebook.com/marketplace/item/mock999888',
                'description': '2 bedroom villa in Singakerta for long-term monthly rent. Indoor kitchen with modern appliances. AC in all rooms, fiber WiFi. Electricity and water excluded. Fully furnished. Contact 081555666777'
            }
        ]
            
            logger.info(f"Returning {len(mock_listings)} mock listings for testing")
            return mock_listings
        
        # REAL APIFY SCRAPING:
        all_listings = []
        
        try:
            for url in self.marketplace_urls:
                logger.info(f"Scraping URL: {url}")
                
                # Run the Apify actor
                run_input = {
                    "startUrls": [{"url": url}],
                    "resultsLimit": self.max_listings,
                    "proxyConfiguration": build_apify_proxy_config(),
                }
                
                # Start the actor and wait for it to finish
                actor_name = "apify/facebook-marketplace-scraper"
                logger.info(f"Starting Apify actor: {actor_name}")
                
                run = self.client.actor(actor_name).call(
                    run_input=run_input
                )
                
                logger.info(f"Actor finished. Status: {run.get('status')}")
                logger.info(f"Duration: {run.get('stats', {}).get('durationMillis', 0) / 1000:.1f} seconds")
                logger.info(f"Cost: ~${run.get('stats', {}).get('computeUnits', 0) * 0.25:.2f}")
                
                # Fetch results from the actor's dataset
                items = []
                for item in self.client.dataset(run["defaultDatasetId"]).iterate_items():
                    items.append(item)
                
                logger.info(f"Scraped {len(items)} listings from {url}")
                all_listings.extend(items)
            
            logger.info(f"Total listings scraped: {len(all_listings)}")
            return all_listings
            
        except Exception as e:
            logger.error(f"Error scraping Apify: {e}")
            return []
    
    def scrape_titles_only(self, max_items: int = 100) -> List[Dict]:
        """
        STAGE 1: Scrape listings using official apify/facebook-marketplace-scraper.
        This actor doesn't have includeSeller parameter, so it returns full data anyway.
        We use it for Stage 1 because it's the official Apify actor (more stable).
        
        Args:
            max_items: Maximum number of items to scrape (default: 100)
            
        Returns:
            List of normalized listing dicts with basic data
        """
        logger.info(f"[STAGE 1] Starting scrape with {self.ACTOR_ID}")
        logger.info(f"[STAGE 1] Max items: {max_items}")
        
        # Build actor input
        run_input = {
            "startUrls": [{"url": url} for url in self.marketplace_urls],
            "resultsLimit": max_items,
            "proxyConfiguration": build_apify_proxy_config(),
        }
        
        logger.info(f"Actor input: {run_input}")
        
        try:
            # Run the actor
            run = self.client.actor(self.ACTOR_ID).call(run_input=run_input)
            
            logger.info(f"Actor run ID: {run['id']}")
            logger.info(f"Actor status: {run['status']}")
            
            # Fetch results
            dataset_id = run['defaultDatasetId']
            items = list(self.client.dataset(dataset_id).iterate_items())
            
            logger.info(f"[STAGE 1] Fetched {len(items)} items")
            
            # Normalize
            normalized = []
            for item in items:
                listing = self.normalize_listing(item)
                if listing:
                    normalized.append(listing)
            
            logger.info(f"[STAGE 1] Normalized {len(normalized)} listings")
            return normalized
            
        except Exception as e:
            logger.error(f"[STAGE 1] Error running actor: {e}")
            raise
    
    def normalize_listing(self, raw_listing: Dict[str, Any]) -> Dict[str, str]:
        """
        Normalize raw listing data to standardized format.
        
        Args:
            raw_listing: Raw listing from Apify
            
        Returns:
            Normalized listing dictionary
        """
        def safe_extract(value, default=''):
            """Safely extract string value from dict or other types."""
            if value is None:
                return default
            if isinstance(value, dict):
                # If it's a dict, try common keys
                if 'text' in value:
                    return str(value['text'])
                if 'label' in value:
                    return str(value['label'])
                if 'value' in value:
                    return str(value['value'])
                # Return string representation as fallback
                return str(value)
            if isinstance(value, list):
                # If it's a list, join elements
                return ', '.join(str(item) for item in value if item)
            return str(value)
        
        # Extract price from listing_price object
        listing_price = raw_listing.get('listing_price', {})
        if isinstance(listing_price, dict):
            price = listing_price.get('formatted_amount', '')
        else:
            price = ''
        
        # Extract location display name
        location = raw_listing.get('location', {})
        if isinstance(location, dict):
            reverse_geocode = location.get('reverse_geocode', {})
            if isinstance(reverse_geocode, dict):
                city_page = reverse_geocode.get('city_page', {})
                if isinstance(city_page, dict):
                    location_str = city_page.get('display_name', '')
                else:
                    location_str = safe_extract(location)
            else:
                location_str = safe_extract(location)
        else:
            location_str = safe_extract(location)
        
        # Normalize the listing
        return {
            'fb_id': safe_extract(raw_listing.get('id', '')),
            'title': safe_extract(raw_listing.get('marketplace_listing_title', '')),
            'price': price,
            'location': location_str,
            'listing_url': safe_extract(raw_listing.get('listingUrl', '')),
            'description': safe_extract(raw_listing.get('marketplace_listing_description', ''))
        }
