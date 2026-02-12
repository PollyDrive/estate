import psycopg2
from psycopg2 import sql
import logging
import os
from typing import Optional, Dict, Any, List

logger = logging.getLogger(__name__)

# Define status constants for clarity
STATUS_STAGE1_NEW = 'stage1_new'  # Legacy, keeping for compatibility
STATUS_STAGE1 = 'stage1'
STATUS_STAGE2 = 'stage2'
STATUS_STAGE2_FAILED = 'stage2_failed'
STATUS_NO_DESCRIPTION = 'no_description'
STATUS_STAGE3 = 'stage3'
STATUS_STAGE3_FAILED = 'stage3_failed'
STATUS_STAGE4 = 'stage4'
STATUS_STAGE4_DUPLICATE = 'stage4_duplicate'
STATUS_STAGE5_SENT = 'stage5_sent'
STATUS_STAGE2_FILTERED = 'stage2_filtered'  # Legacy
STATUS_STAGE2_REJECTED = 'stage2_rejected'  # Legacy
STATUS_STAGE3_ANALYZED = 'stage3_analyzed'  # Legacy
STATUS_DUPLICATE = 'rejected_duplicate'  # Legacy


class Database:
    """
    Database manager for PostgreSQL operations, designed to work with a unified,
    status-driven 'listings' table.
    """
    
    def __init__(self):
        """
        Initialize database connection details from environment variables.
        """
        self.db_host = os.getenv('POSTGRES_HOST')
        self.db_port = os.getenv('POSTGRES_PORT')
        self.db_name = os.getenv('POSTGRES_DB')
        self.db_user = os.getenv('POSTGRES_USER')
        self.db_password = os.getenv('POSTGRES_PASSWORD')
        self.db_schema = os.getenv('POSTGRES_SCHEMA')
        self.conn = None
        self.cursor = None
    
    def connect(self):
        """Establish database connection."""
        if not all([self.db_host, self.db_port, self.db_name, self.db_user, self.db_password]):
            logger.error("One or more database environment variables are not set!")
            raise ValueError("Database connection details are missing from environment.")
        
        try:
            self.conn = psycopg2.connect(
                host=self.db_host,
                port=self.db_port,
                dbname=self.db_name,
                user=self.db_user,
                password=self.db_password
            )
            self.cursor = self.conn.cursor()
            if self.db_schema:
                self.cursor.execute(
                    sql.SQL("SET search_path TO {}, public").format(sql.Identifier(self.db_schema))
                )
                logger.info(f"Database search_path set to schema '{self.db_schema}'")
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
        source: str = 'apify-marketplace',
        group_id: str = None,
        description: str = None
    ) -> bool:
        """
        Adds a new listing from the initial scrape (Stage 1).
        If the listing already exists, it does nothing.
        """
        query = """
            INSERT INTO listings (fb_id, title, price, location, listing_url, status, source, group_id, description)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (fb_id) DO NOTHING
        """
        try:
            self.cursor.execute(query, (fb_id, title, price, location, listing_url, STATUS_STAGE1, source, group_id, description))
            self.conn.commit()
            inserted_count = self.cursor.rowcount
            if inserted_count > 0:
                logger.info(f"Stage 1: New listing {fb_id} added to database with source '{source}'.")
                return True
            return False
        except Exception as e:
            logger.error(f"Error adding Stage 1 listing {fb_id}: {e}")
            self.conn.rollback()
            return False

    def get_listings_for_stage2(self) -> List[Dict[str, Any]]:
        """
        Gets all listings that are new and ready for detailed scraping (Stage 2).
        """
        query = "SELECT fb_id, listing_url, source FROM listings WHERE status IN (%s, %s) ORDER BY created_at DESC"
        try:
            self.cursor.execute(query, (STATUS_STAGE1, STATUS_STAGE1_NEW))  # Support both old and new status
            columns = [desc[0] for desc in self.cursor.description]
            return [dict(zip(columns, row)) for row in self.cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting listings for Stage 2: {e}")
            return []

    def update_listing_after_stage2(self, fb_id: str, details: Dict[str, Any], passed: bool):
        """
        Updates a listing with detailed information after Stage 2 scraping and filtering.
        """
        new_status = STATUS_STAGE2_FILTERED if passed else STATUS_STAGE2_REJECTED
        details['status'] = new_status
        
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
        """
        query = "SELECT * FROM listings WHERE status = %s ORDER BY created_at DESC"
        try:
            self.cursor.execute(query, (STATUS_STAGE2_FILTERED,))
            columns = [desc[0] for desc in self.cursor.description]
            return [dict(zip(columns, row)) for row in self.cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting listings for Stage 3: {e}")
            return []

    def update_listing_after_stage3(self, fb_id: str, llm_passed: bool, llm_reason: str):
        """
        Updates a listing with the results of the LLM analysis (Stage 3).
        """
        query = """
            UPDATE listings 
            SET status = %s, llm_passed = %s, llm_reason = %s, llm_analyzed_at = NOW()
            WHERE fb_id = %s
        """
        try:
            self.cursor.execute(query, (STATUS_STAGE3_ANALYZED, llm_passed, llm_reason, fb_id))
            self.conn.commit()
            logger.info(f"Stage 3: Updated listing {fb_id} with LLM analysis results.")
        except Exception as e:
            logger.error(f"Error updating listing {fb_id} after Stage 3: {e}")
            self.conn.rollback()

    def get_listings_for_telegram(self) -> List[Dict[str, Any]]:
        """
        Gets all listings that have passed all stages and are ready to be sent to Telegram.
        """
        query = """
            SELECT * FROM listings 
            WHERE status = %s AND llm_passed = TRUE AND telegram_sent = FALSE
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
        """
        query = "UPDATE listings SET telegram_sent = TRUE, telegram_sent_at = NOW() WHERE fb_id = %s"
        try:
            self.cursor.execute(query, (fb_id,))
            self.conn.commit()
            logger.info(f"Marked listing {fb_id} as sent to Telegram.")
        except Exception as e:
            logger.error(f"Error marking listing {fb_id} as sent: {e}")
            self.conn.rollback()

    def delete_listing(self, fb_id: str):
        """
        Deletes a listing from the database, e.g., if it's unavailable.
        """
        query = "DELETE FROM listings WHERE fb_id = %s"
        try:
            self.cursor.execute(query, (fb_id,))
            self.conn.commit()
            logger.warning(f"DELETED listing {fb_id} from database.")
        except Exception as e:
            logger.error(f"Error deleting listing {fb_id}: {e}")
            self.conn.rollback()

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
