import logging
from typing import List, Dict, Any
from apify_client import ApifyClient

logger = logging.getLogger(__name__)


class FacebookGroupScraper:
    """
    Apify Facebook Group scraper.
    Uses the official 'apify/facebook-groups-scraper' actor.
    """
    
    ACTOR_ID = "apify/facebook-groups-scraper"
    
    def __init__(self, api_key: str, config: Dict):
        """
        Initialize Apify Group scraper.
        
        Args:
            api_key: Apify API key.
            config: Configuration dictionary.
        """
        self.client = ApifyClient(api_key)
        self.config = config.get('facebook_groups', {})
    
    def scrape_posts(self, group_ids: List[str]) -> List[Dict[str, Any]]:
        """
        Scrapes the latest posts from a list of Facebook groups.
        
        Args:
            group_ids: A list of Facebook Group IDs to scrape.
            
        Returns:
            A list of normalized post dictionaries.
        """
        logger.info(f"[GROUPS] Starting scrape with actor: {self.ACTOR_ID}")
        
        max_posts_per_group = self.config.get('max_posts_per_group', 15)
        
        # The actor expects 'startUrls' with group URLs
        start_urls = [{"url": f"https://www.facebook.com/groups/{group_id}/"} for group_id in group_ids]
        
        # Calculate total limit based on number of groups and posts per group
        results_limit = len(group_ids) * max_posts_per_group
        
        run_input = {
            "startUrls": start_urls,
            "resultsLimit": 100,
            "viewOption": "CHRONOLOGICAL",
            "maxPostCount": max_posts_per_group,
            "proxyConfiguration": { "useApifyProxy": True },
        }
        
        logger.info(f"Actor input: {run_input}")
        
        try:
            run = self.client.actor(self.ACTOR_ID).call(run_input=run_input)
            
            logger.info(f"Actor run ID: {run['id']}")
            logger.info(f"Actor status: {run['status']}")
            
            dataset_id = run['defaultDatasetId']
            items = list(self.client.dataset(dataset_id).iterate_items())
            
            logger.info(f"[GROUPS] Fetched {len(items)} total posts.")
            
            normalized_posts = [self.normalize_post(post) for post in items]
            return [p for p in normalized_posts if p]

        except Exception as e:
            logger.error(f"[GROUPS] Error running group scraper actor: {e}")
            raise

    def fetch_results_from_run(self, run_id: str) -> List[Dict[str, Any]]:
        """
        Fetches results from a specific, previously completed actor run.
        
        Args:
            run_id: The ID of the Apify actor run.
            
        Returns:
            A list of normalized post dictionaries.
        """
        logger.info(f"[GROUPS] Fetching results from existing run: {run_id}")
        try:
            run = self.client.run(run_id).get()
            if not run:
                logger.error(f"Run {run_id} not found.")
                return []

            dataset_id = run['defaultDatasetId']
            logger.info(f"Found dataset {dataset_id} for run {run_id}")

            items = list(self.client.dataset(dataset_id).iterate_items())
            logger.info(f"[GROUPS] Fetched {len(items)} total posts from dataset.")
            
            normalized_posts = [self.normalize_post(post) for post in items]
            return [p for p in normalized_posts if p]
            
        except Exception as e:
            logger.error(f"[GROUPS] Error fetching results from run {run_id}: {e}")
            raise

    def normalize_post(self, raw_post: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalizes raw post data from the actor to our standardized format.
        
        Args:
            raw_post: Raw post data from Apify.
            
        Returns:
            A normalized dictionary or None if essential data is missing.
        """
        post_url = raw_post.get('url')
        if not post_url:
            return None

        # Extract a unique ID from the post URL
        try:
            fb_id = raw_post.get('postId')
            if not fb_id:
                # Fallback for different URL structures
                if 'permalink.php' in post_url:
                    fb_id = raw_post.get('story_fbid')
                else:
                    parts = post_url.strip('/').split('/')
                    fb_id = parts[-1] if parts[-1].isdigit() else None
            
            if not fb_id:
                 # Final fallback
                fb_id = f"group_post_{raw_post.get('post_id', post_url.replace('/', '_'))}"

        except Exception:
            return None

        text = raw_post.get('text', '')

        return {
            'fb_id': fb_id,
            'group_id': raw_post.get('groupId'),
            'title': text,
            'description': text,
            'listing_url': post_url,
            'price': '',
            'location': '',
            'timestamp': raw_post.get('time'),
            'all_images': raw_post.get('images', [])
        }
