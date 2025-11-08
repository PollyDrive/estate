import psycopg2
import logging
from typing import Optional, Dict, Any, List

logger = logging.getLogger(__name__)

# Define status constants for clarity
STATUS_STAGE1_NEW = 'stage1_new'
STATUS_STAGE2_FILTERED = 'stage2_filtered'
STATUS_STAGE2_REJECTED = 'stage2_rejected'
STATUS_STAGE3_ANALYZED = 'stage3_analyzed'


class Database:
    """
    Database manager for PostgreSQL operations, designed to work with a unified,
    status-driven 'listings' table.
    """
    
    def __init__(self, db_url: str):
        """
        Initialize database connection.
        
        Args:
            db_url: PostgreSQL connection URL.
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

    def add_listing_from_stage1(
        self,
        fb_id: str,
        title: str,
        price: str,
        location: str,
        listing_url: str,
        pass_reason: str
    ) -> bool:
        """
        Adds a new listing from the initial scrape (Stage 1).
        If the listing already exists, it does nothing.
        
        Args:
            fb_id: Facebook listing ID.
            title: Listing title.
            price: Price text.
            location: Location text.
            listing_url: Full URL.
            pass_reason: Why it passed Stage 1 filters.
            
        Returns:
            True if a new record was inserted, False otherwise.
        """
        query = """
            INSERT INTO listings (fb_id, title, price, location, listing_url, pass_reason, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (fb_id) DO NOTHING
        """
        try:
            self.cursor.execute(query, (fb_id, title, price, location, listing_url, pass_reason, STATUS_STAGE1_NEW))
            self.conn.commit()
            inserted_count = self.cursor.rowcount
            if inserted_count > 0:
                logger.info(f"Stage 1: New listing {fb_id} added to database.")
                return True
            return False
        except Exception as e:
            logger.error(f"Error adding Stage 1 listing {fb_id}: {e}")
            self.conn.rollback()
            return False

    def get_listings_for_stage2(self) -> List[Dict[str, Any]]:
        """
        Gets all listings that are new and ready for detailed scraping (Stage 2).
        
        Returns:
            A list of dictionaries, each representing a listing.
        """
        query = "SELECT fb_id, listing_url FROM listings WHERE status = %s ORDER BY created_at DESC"
        try:
            self.cursor.execute(query, (STATUS_STAGE1_NEW,))
            columns = [desc[0] for desc in self.cursor.description]
            return [dict(zip(columns, row)) for row in self.cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting listings for Stage 2: {e}")
            return []

    def update_listing_after_stage2(self, fb_id: str, details: Dict[str, Any], passed: bool):
        """
        Updates a listing with detailed information after Stage 2 scraping and filtering.
        
        Args:
            fb_id: The Facebook ID of the listing to update.
            details: A dictionary containing all the scraped details.
            passed: A boolean indicating if the listing passed the simple filters.
        """
        new_status = STATUS_STAGE2_FILTERED if passed else STATUS_STAGE2_REJECTED
        details['status'] = new_status
        
        # Build the SET part of the UPDATE query dynamically from the details dict
        set_clause = ", ".join([f"{key} = %s" for key in details.keys()])
        query = f"UPDATE listings SET {set_clause} WHERE fb_id = %s"
        
        values = list(details.values())
        values.append(fb_id)
        
        try:
            self.cursor.execute(query, tuple(values))
            self.conn.commit()
            logger.info(f"Stage 2: Updated listing {fb_id} with status '{new_status}'.")
        except Exception as e:
            logger.error(f"Error updating listing {fb_id} after Stage 2: {e}")
            self.conn.rollback()

    def get_listings_for_stage3(self) -> List[Dict[str, Any]]:
        """
        Gets all listings that have passed Stage 2 and are ready for LLM analysis (Stage 3).
        
        Returns:
            A list of listing dictionaries.
        """
        query = "SELECT * FROM listings WHERE status = %s ORDER BY created_at DESC"
        try:
            self.cursor.execute(query, (STATUS_STAGE2_FILTERED,))
            columns = [desc[0] for desc in self.cursor.description]
            return [dict(zip(columns, row)) for row in self.cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting listings for Stage 3: {e}")
            return []

    def update_listing_after_stage3(self, fb_id: str, groq_passed: bool, groq_reason: str):
        """
        Updates a listing with the results of the LLM analysis (Stage 3).
        
        Args:
            fb_id: The Facebook ID of the listing.
            groq_passed: Whether the listing passed the Groq LLM filter.
            groq_reason: The reason provided by the LLM.
        """
        query = """
            UPDATE listings 
            SET status = %s, groq_passed = %s, groq_reason = %s, groq_analyzed_at = NOW()
            WHERE fb_id = %s
        """
        try:
            self.cursor.execute(query, (STATUS_STAGE3_ANALYZED, groq_passed, groq_reason, fb_id))
            self.conn.commit()
            logger.info(f"Stage 3: Updated listing {fb_id} with LLM analysis results.")
        except Exception as e:
            logger.error(f"Error updating listing {fb_id} after Stage 3: {e}")
            self.conn.rollback()

    def get_listings_for_telegram(self) -> List[Dict[str, Any]]:
        """
        Gets all listings that have passed all stages and are ready to be sent to Telegram.
        
        Returns:
            A list of listing dictionaries.
        """
        query = """
            SELECT * FROM listings 
            WHERE status = %s AND groq_passed = TRUE AND telegram_sent = FALSE
            ORDER BY created_at DESC
        """
        try:
            self.cursor.execute(query, (STATUS_STAGE3_ANALYZED,))
            columns = [desc[0] for desc in self.cursor.description]
            return [dict(zip(columns, row)) for row in self.cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting listings for Telegram: {e}")
            return []

    def mark_listing_sent(self, fb_id: str):
        """
        Marks a listing as sent to Telegram.
        
        Args:
            fb_id: The Facebook ID of the listing.
        """
        query = "UPDATE listings SET telegram_sent = TRUE, telegram_sent_at = NOW() WHERE fb_id = %s"
        try:
            self.cursor.execute(query, (fb_id,))
            self.conn.commit()
            logger.info(f"Marked listing {fb_id} as sent to Telegram.")
        except Exception as e:
            logger.error(f"Error marking listing {fb_id} as sent: {e}")
            self.conn.rollback()

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()