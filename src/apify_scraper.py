import logging
from typing import List, Dict, Any
from apify_client import ApifyClient

logger = logging.getLogger(__name__)


class ApifyScraper:
    """Apify Facebook Marketplace scraper."""
    
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
        all_listings = []
        
        try:
            for url in self.marketplace_urls:
                logger.info(f"Scraping URL: {url}")
                
                # Run the Apify actor
                run_input = {
                    "startUrls": [{"url": url}],
                    "maxItems": self.max_listings,
                    "proxyConfiguration": {
                        "useApifyProxy": True
                    }
                }
                
                # Start the actor and wait for it to finish
                run = self.client.actor("petrpatek/facebook-marketplace-scraper").call(
                    run_input=run_input
                )
                
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
    
    def normalize_listing(self, raw_listing: Dict[str, Any]) -> Dict[str, str]:
        """
        Normalize raw listing data to standardized format.
        
        Args:
            raw_listing: Raw listing from Apify
            
        Returns:
            Normalized listing dictionary
        """
        # Apify Facebook Marketplace scraper returns different field names
        # Adjust these based on actual Apify output structure
        return {
            'fb_id': raw_listing.get('id', ''),
            'title': raw_listing.get('title', ''),
            'price': raw_listing.get('price', ''),
            'location': raw_listing.get('location', ''),
            'listing_url': raw_listing.get('url', ''),
            'description': raw_listing.get('description', '')
        }
