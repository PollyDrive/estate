import psycopg2
import os
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class Database:
    """Database manager for PostgreSQL operations."""
    
    def __init__(self, db_url: str):
        """
        Initialize database connection.
        
        Args:
            db_url: PostgreSQL connection URL
        """
        self.db_url = db_url
        self.conn = None
        self.cursor = None
    
    def connect(self):
        """Establish database connection."""
        try:
            self.conn = psycopg2.connect(self.db_url)
            self.cursor = self.conn.cursor()
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def close(self):
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Database connection closed")
    
    def listing_exists(self, fb_id: str) -> bool:
        """
        Check if listing already exists in database.
        
        Args:
            fb_id: Facebook listing ID
            
        Returns:
            True if listing exists, False otherwise
        """
        try:
            query = "SELECT 1 FROM fb_listings WHERE fb_id = %s"
            self.cursor.execute(query, (fb_id,))
            result = self.cursor.fetchone()
            return result is not None
        except Exception as e:
            logger.error(f"Error checking listing existence: {e}")
            return False
    
    def insert_listing(
        self,
        fb_id: str,
        title: str,
        price: str,
        location: str,
        listing_url: str,
        description: str,
        phone_number: Optional[str] = None,
        sent_to_telegram: bool = False
    ) -> bool:
        """
        Insert new listing into database.
        
        Args:
            fb_id: Facebook listing ID
            title: Listing title
            price: Listing price
            location: Listing location
            listing_url: URL to listing
            description: Listing description
            phone_number: Phone number if found
            sent_to_telegram: Whether listing was sent to Telegram
            
        Returns:
            True if insert successful, False otherwise
        """
        try:
            query = """
                INSERT INTO fb_listings 
                (fb_id, title, price, location, listing_url, description, phone_number, sent_to_telegram)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            self.cursor.execute(
                query,
                (fb_id, title, price, location, listing_url, description, phone_number, sent_to_telegram)
            )
            self.conn.commit()
            logger.info(f"Listing {fb_id} inserted into database")
            return True
        except Exception as e:
            logger.error(f"Error inserting listing: {e}")
            self.conn.rollback()
            return False
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
