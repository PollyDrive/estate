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

    def update_listing_after_stage3(self, fb_id: str, llm_passed: bool, llm_reason: str, llm_model: str = None):
        """
        Updates a listing with the results of the LLM analysis (Stage 3).
        """
        query = """
            UPDATE listings
            SET status = %s, llm_passed = %s, llm_reason = %s, llm_model = %s, llm_analyzed_at = NOW()
            WHERE fb_id = %s
        """
        try:
            self.cursor.execute(query, (STATUS_STAGE3_ANALYZED, llm_passed, llm_reason, llm_model, fb_id))
            self.conn.commit()
            logger.info(f"Stage 3: Updated listing {fb_id} with LLM analysis results (model: {llm_model}).")
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

    def save_telegram_message_id(self, fb_id: str, message_id: int):
        """
        Save telegram message_id after sending a listing.

        Args:
            fb_id: Facebook listing ID
            message_id: Telegram message ID
        """
        query = "UPDATE listings SET telegram_message_id = %s WHERE fb_id = %s"
        try:
            self.cursor.execute(query, (message_id, fb_id))
            self.conn.commit()
            logger.info(f"Saved telegram_message_id {message_id} for listing {fb_id}")
        except Exception as e:
            logger.error(f"Error saving telegram_message_id for {fb_id}: {e}")
            self.conn.rollback()

    def log_batch_start(self, batch_date, batch_number: int) -> Optional[int]:
        """
        Log the start of a Stage5 batch run.

        Args:
            batch_date: Date of the batch (date object)
            batch_number: Sequential batch number for this date

        Returns:
            batch_run_id if successful, None otherwise
        """
        query = """
            INSERT INTO batch_runs (batch_date, batch_number, started_at, status)
            VALUES (%s, %s, NOW(), 'running')
            RETURNING id
        """
        try:
            self.cursor.execute(query, (batch_date, batch_number))
            self.conn.commit()
            batch_run_id = self.cursor.fetchone()[0]
            logger.info(f"Started batch run {batch_run_id} (date: {batch_date}, batch #{batch_number})")
            return batch_run_id
        except Exception as e:
            logger.error(f"Error logging batch start: {e}")
            self.conn.rollback()
            return None

    def log_batch_complete(
        self,
        batch_run_id: int,
        listings_sent: int,
        no_desc_sent: int,
        blocked_count: int = 0,
        error_count: int = 0
    ):
        """
        Mark a batch run as completed.

        Args:
            batch_run_id: ID of the batch run
            listings_sent: Number of regular listings sent
            no_desc_sent: Number of no-description listings sent
            blocked_count: Number of listings blocked by stage5 guard
            error_count: Number of errors encountered
        """
        query = """
            UPDATE batch_runs
            SET finished_at = NOW(),
                listings_sent = %s,
                no_desc_sent = %s,
                blocked_count = %s,
                error_count = %s,
                status = 'completed'
            WHERE id = %s
        """
        try:
            self.cursor.execute(query, (listings_sent, no_desc_sent, blocked_count, error_count, batch_run_id))
            self.conn.commit()
            logger.info(f"Completed batch run {batch_run_id}: {listings_sent} sent, {no_desc_sent} no-desc, {blocked_count} blocked, {error_count} errors")
        except Exception as e:
            logger.error(f"Error logging batch completion: {e}")
            self.conn.rollback()

    def get_batch_count_today(self, chat_id: Optional[int] = None) -> int:
        """
        Get the number of completed batches today for a specific chat.

        Args:
            chat_id: Telegram chat ID to filter by (required for per-chat stats).
                     If None, returns global count across all chats (legacy).

        Returns:
            Number of completed batches for today for the given chat.
        """
        try:
            self.conn.rollback()  # reset any aborted transaction from a prior query
            if chat_id is not None:
                self.cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM batch_runs
                    WHERE batch_date = CURRENT_DATE
                      AND status = 'completed'
                      AND chat_id = %s
                    """,
                    (chat_id,)
                )
            else:
                self.cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM batch_runs
                    WHERE batch_date = CURRENT_DATE
                      AND status = 'completed'
                    """
                )
            count = self.cursor.fetchone()[0]

            # Fallback: estimate from listing_profiles sent today for this chat
            if count == 0:
                if chat_id is not None:
                    self.cursor.execute(
                        """
                        SELECT COUNT(*)
                        FROM listing_profiles
                        WHERE chat_id = %s
                          AND sent_at::date = CURRENT_DATE
                        """,
                        (chat_id,)
                    )
                else:
                    self.cursor.execute(
                        """
                        SELECT COUNT(*)
                        FROM listing_profiles
                        WHERE sent_at::date = CURRENT_DATE
                        """
                    )
                sent_count = self.cursor.fetchone()[0]
                return (sent_count + 9) // 10  # Round up to nearest batch

            return count or 0
        except Exception as e:
            logger.error(f"Error getting batch count: {e}")
            return 0

    def get_sent_listings_count_today(self, chat_id: Optional[int] = None) -> int:
        """
        Get total number of listings sent today for a specific chat.

        Uses listing_profiles.sent_at as the authoritative source —
        listings.updated_at is unreliable (not updated on re-send to new chats).

        Args:
            chat_id: Telegram chat ID to filter by. If None, counts across all chats.

        Returns:
            Number of listings sent today for the given chat.
        """
        try:
            self.conn.rollback()  # reset any aborted transaction from a prior query
            if chat_id is not None:
                self.cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM listing_profiles
                    WHERE chat_id = %s
                      AND sent_at::date = CURRENT_DATE
                    """,
                    (chat_id,)
                )
            else:
                self.cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM listing_profiles
                    WHERE sent_at::date = CURRENT_DATE
                    """
                )
            count = self.cursor.fetchone()[0]
            return count or 0
        except Exception as e:
            logger.error(f"Error getting sent listings count: {e}")
            return 0

    def save_reaction(self, telegram_message_id: int, fb_id: str, reaction_type: str, chat_id: Optional[int] = None):
        """
        Save or increment a reaction count.

        Args:
            telegram_message_id: Telegram message ID
            fb_id: Facebook listing ID
            reaction_type: Emoji reaction ('❤', '💩', '🤡')
            chat_id: Telegram chat ID (profile identifier)
        """
        query = """
            INSERT INTO feedback (telegram_message_id, fb_id, reaction_type, reaction_count, first_seen_at, last_updated_at, chat_id)
            VALUES (%s, %s, %s, 1, NOW(), NOW(), %s)
            ON CONFLICT (telegram_message_id, reaction_type)
            DO UPDATE SET
                reaction_count = feedback.reaction_count + 1,
                last_updated_at = NOW(),
                chat_id = COALESCE(EXCLUDED.chat_id, feedback.chat_id)
        """
        try:
            self.cursor.execute(query, (telegram_message_id, fb_id, reaction_type, chat_id))
            self.conn.commit()
            logger.info(f"Saved reaction {reaction_type} for message {telegram_message_id} (fb_id: {fb_id})")
        except Exception as e:
            logger.error(f"Error saving reaction: {e}")
            self.conn.rollback()

    def get_feedback_stats(self, since=None, chat_id: Optional[int] = None) -> Dict[str, Any]:
        """
        Get aggregated feedback statistics for listings sent to a specific chat.

        Uses listing_profiles.sent_at as the authoritative send-date source.
        listings.updated_at is NOT used (it reflects the last write to the listings
        row, not the per-chat send time).

        Args:
            since: Optional date to filter by send date (default: today).
                   Pass a datetime object; its .date() is used.
            chat_id: Telegram chat ID to filter by (required for correct stats).
                     If None, returns stats across all chats.

        Returns:
            Dictionary with feedback statistics keyed by reaction emoji.
        """
        self.conn.rollback()  # reset any aborted transaction from a prior query

        if since is None:
            date_expr = "CURRENT_DATE"
        else:
            date_expr = f"'{since.date()}'"

        # Filter on listing_profiles for the correct per-chat send timestamp.
        # For reactions: match by fb_id (not chat_id on feedback) because
        # feedback.chat_id may be NULL for older rows recorded before the fix.
        # We trust that if a listing was sent to this chat, its reactions belong here.
        params: list = []

        if chat_id is not None:
            lp_filter = "lp.chat_id = %s AND lp.sent_at::date = " + date_expr
            params.append(chat_id)
        else:
            lp_filter = "lp.sent_at::date = " + date_expr

        # Use DISTINCT on fb_id inside aggregation to guard against double-counting
        # if the same listing is ever sent to multiple chats in the future.
        query = f"""
            SELECT
                f.reaction_type,
                COUNT(DISTINCT f.telegram_message_id) as message_count,
                SUM(f.reaction_count) as total_reactions,
                json_agg(DISTINCT
                    json_build_object(
                        'fb_id', l.fb_id,
                        'listing_url', l.listing_url,
                        'title', l.title,
                        'reaction_count', f.reaction_count
                    )
                ) as listings
            FROM listing_profiles lp
            JOIN listings l ON l.fb_id = lp.fb_id
            JOIN feedback f ON f.fb_id = l.fb_id
            WHERE {lp_filter}
              AND f.reaction_type IS NOT NULL
            GROUP BY f.reaction_type
        """

        try:
            self.cursor.execute(query, params if params else None)
            rows = self.cursor.fetchall()

            # Note: Telegram sends ❤ (U+2764) without variation selector
            stats = {
                '❤': {'message_count': 0, 'total_reactions': 0, 'listings': []},
                '💩': {'message_count': 0, 'total_reactions': 0, 'listings': []},
                '🤡': {'message_count': 0, 'total_reactions': 0, 'listings': []}
            }

            for row in rows:
                reaction_type, message_count, total_reactions, listings = row
                if reaction_type in stats:
                    stats[reaction_type] = {
                        'message_count': message_count,
                        'total_reactions': total_reactions,
                        'listings': listings or []
                    }

            return stats
        except Exception as e:
            logger.error(f"Error getting feedback stats: {e}")
            self.conn.rollback()
            return {}

    def get_fb_id_by_message_id(self, telegram_message_id: int) -> Optional[str]:
        """
        Get fb_id by telegram_message_id.

        Looks up in listing_profiles first (per-chat message_id, set by run_stage5),
        then falls back to listings.telegram_message_id (legacy / single-chat path).

        Args:
            telegram_message_id: Telegram message ID

        Returns:
            fb_id if found, None otherwise
        """
        query = """
            SELECT fb_id FROM listing_profiles
            WHERE telegram_message_id = %s
            LIMIT 1
        """
        try:
            self.conn.rollback()  # reset any aborted transaction from a prior query
            self.cursor.execute(query, (telegram_message_id,))
            result = self.cursor.fetchone()
            if result:
                return result[0]
            # Fallback: listings table (legacy)
            self.cursor.execute(
                "SELECT fb_id FROM listings WHERE telegram_message_id = %s LIMIT 1",
                (telegram_message_id,)
            )
            result = self.cursor.fetchone()
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Error getting fb_id by message_id {telegram_message_id}: {e}")
            return None

    def get_favorite_listings(self, limit: int = 50, chat_id: Optional[int] = None) -> list:
        """
        Get all listings with ❤ reactions, optionally filtered by chat_id.

        Filters by listing_profiles.chat_id (not feedback.chat_id) to correctly
        attribute reactions to the chat that received the listing, even when
        feedback.chat_id is NULL (older rows recorded before the fix).
        """
        if chat_id is not None:
            query = """
                SELECT
                    l.fb_id,
                    l.title,
                    l.listing_url,
                    l.price,
                    l.location,
                    f.reaction_count,
                    f.first_seen_at,
                    lp.sent_at
                FROM feedback f
                JOIN listings l ON f.fb_id = l.fb_id
                JOIN listing_profiles lp ON lp.fb_id = l.fb_id AND lp.chat_id = %s
                WHERE f.reaction_type = '❤'
                ORDER BY f.first_seen_at DESC
                LIMIT %s
            """
            params = [chat_id, limit]
        else:
            query = """
                SELECT
                    l.fb_id,
                    l.title,
                    l.listing_url,
                    l.price,
                    l.location,
                    f.reaction_count,
                    f.first_seen_at,
                    l.updated_at as sent_at
                FROM feedback f
                JOIN listings l ON f.fb_id = l.fb_id
                WHERE f.reaction_type = '❤'
                ORDER BY f.first_seen_at DESC
                LIMIT %s
            """
            params = [limit]
        try:
            self.conn.rollback()  # reset any aborted transaction from a prior query
            self.cursor.execute(query, params)
            rows = self.cursor.fetchall()
            favorites = []
            for row in rows:
                fb_id, title, url, price, location, count, first_seen, sent_at = row
                favorites.append({
                    'fb_id': fb_id, 'title': title, 'url': url,
                    'price': price, 'location': location,
                    'reaction_count': count, 'first_seen_at': first_seen, 'sent_at': sent_at
                })
            return favorites
        except Exception as e:
            logger.error(f"Error getting favorite listings: {e}")
            return []

    # ── Chat profiles ─────────────────────────────────────────────────────────

    def register_chat(self, chat_id: int, name: str = '') -> bool:
        """
        Register a new chat as a pending profile (enabled=FALSE) if not already known.
        Returns True if a new record was created, False if already exists.
        """
        query = """
            INSERT INTO chat_profiles
                (chat_id, name, enabled, bedrooms_min, price_max,
                 allowed_locations, stop_locations, qfr_start_urls)
            VALUES (%s, %s, FALSE, 1, 40000000, '[]', '[]', '[]')
            ON CONFLICT (chat_id) DO NOTHING
        """
        try:
            self.cursor.execute(query, (chat_id, name or f'chat_{chat_id}'))
            self.conn.commit()
            created = self.cursor.rowcount > 0
            if created:
                logger.info(f"Registered new pending chat profile: chat_id={chat_id}, name='{name}'")
            return created
        except Exception as e:
            logger.error(f"Error registering chat {chat_id}: {e}")
            self.conn.rollback()
            return False

    def sync_chat_profiles(self, profiles: list) -> None:
        """Upsert chat_profiles from config into DB."""
        import json as _json
        query = """
            INSERT INTO chat_profiles
                (chat_id, name, enabled, bedrooms_min, bedrooms_max, price_max,
                 allowed_locations, stop_locations, qfr_start_urls, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (chat_id) DO UPDATE SET
                name             = EXCLUDED.name,
                enabled          = EXCLUDED.enabled,
                bedrooms_min     = EXCLUDED.bedrooms_min,
                bedrooms_max     = EXCLUDED.bedrooms_max,
                price_max        = EXCLUDED.price_max,
                allowed_locations= EXCLUDED.allowed_locations,
                stop_locations   = EXCLUDED.stop_locations,
                qfr_start_urls   = EXCLUDED.qfr_start_urls,
                updated_at       = NOW()
        """
        try:
            for p in profiles:
                self.cursor.execute(query, (
                    p['chat_id'],
                    p.get('name', ''),
                    p.get('enabled', True),
                    p['bedrooms_min'],
                    p.get('bedrooms_max'),  # optional — None means no upper limit
                    p['price_max'],
                    _json.dumps(p.get('allowed_locations', [])),
                    _json.dumps(p.get('stop_locations', [])),
                    _json.dumps(p.get('qfr_start_urls', [])),
                ))
            self.conn.commit()
            logger.info(f"Synced {len(profiles)} chat profiles to DB")
        except Exception as e:
            logger.error(f"Error syncing chat profiles: {e}")
            self.conn.rollback()

    def get_enabled_chat_profiles(self) -> list:
        """Return all enabled profiles from chat_profiles."""
        query = """
            SELECT chat_id, name, bedrooms_min, bedrooms_max, price_max,
                   allowed_locations, stop_locations, qfr_start_urls
            FROM chat_profiles
            WHERE enabled = TRUE
            ORDER BY chat_id
        """
        try:
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            profiles = []
            for row in rows:
                chat_id, name, br_min, br_max, price_max, allowed, stop, urls = row
                profiles.append({
                    'chat_id': chat_id, 'name': name,
                    'bedrooms_min': br_min, 'bedrooms_max': br_max,
                    'price_max': price_max,
                    'allowed_locations': allowed or [],
                    'stop_locations': stop or [],
                    'qfr_start_urls': urls or [],
                })
            return profiles
        except Exception as e:
            logger.error(f"Error getting chat profiles: {e}")
            return []

    def get_chat_profile(self, chat_id: int) -> Optional[Dict]:
        """Return a single profile by chat_id."""
        query = """
            SELECT chat_id, name, bedrooms_min, bedrooms_max, price_max,
                   allowed_locations, stop_locations, qfr_start_urls
            FROM chat_profiles WHERE chat_id = %s
        """
        try:
            self.cursor.execute(query, (chat_id,))
            row = self.cursor.fetchone()
            if not row:
                return None
            chat_id, name, br_min, br_max, price_max, allowed, stop, urls = row
            return {
                'chat_id': chat_id, 'name': name,
                'bedrooms_min': br_min, 'bedrooms_max': br_max,
                'price_max': price_max,
                'allowed_locations': allowed or [],
                'stop_locations': stop or [],
                'qfr_start_urls': urls or [],
            }
        except Exception as e:
            logger.error(f"Error getting chat profile {chat_id}: {e}")
            return None

    # ── Listing profiles ──────────────────────────────────────────────────────

    def get_listings_for_profile_check(self) -> list:
        """
        Return all listings that passed the global LLM gate (status IN stage3/stage4/stage5_sent)
        so that _check_profile_criteria can be applied retroactively for any newly added profile.
        """
        query = """
            SELECT fb_id, title, price_extracted, location, bedrooms, description
            FROM listings
            WHERE status IN ('stage3', 'stage4', 'stage5_sent')
              AND llm_passed = TRUE
        """
        try:
            self.cursor.execute(query)
            columns = [desc[0] for desc in self.cursor.description]
            return [dict(zip(columns, row)) for row in self.cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error fetching listings for profile check: {e}")
            return []

    def upsert_listing_profile(self, fb_id: str, chat_id: int, passed: bool, reason: Optional[str] = None) -> None:
        """INSERT or UPDATE a listing × profile result."""
        query = """
            INSERT INTO listing_profiles (fb_id, chat_id, passed, reason)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (fb_id, chat_id) DO UPDATE SET
                passed = EXCLUDED.passed,
                reason = EXCLUDED.reason
        """
        try:
            self.cursor.execute(query, (fb_id, chat_id, passed, reason))
            self.conn.commit()
        except Exception as e:
            logger.error(f"Error upserting listing_profile ({fb_id}, {chat_id}): {e}")
            self.conn.rollback()

    def get_listings_for_chat(self, chat_id: int, batch_size: int = 10) -> list:
        """Return pending (passed, not sent) listings for a specific chat."""
        query = """
            SELECT l.fb_id, l.title, l.price, l.price_extracted, l.location,
                   l.listing_url, l.description, l.phone_number, l.bedrooms,
                   l.has_pool, l.has_parking, l.has_wifi, l.has_ac,
                   l.furniture, l.utilities, l.summary_ru, l.source,
                   l.telegram_message_id, lp.id as lp_id
            FROM listings l
            JOIN listing_profiles lp ON l.fb_id = lp.fb_id
            WHERE lp.chat_id = %s
              AND lp.passed = TRUE
              AND lp.sent_at IS NULL
              AND l.status IN ('stage4', 'stage5_sent')
            ORDER BY l.created_at ASC
            LIMIT %s
        """
        try:
            self.cursor.execute(query, (chat_id, batch_size))
            columns = [desc[0] for desc in self.cursor.description]
            rows = self.cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
        except Exception as e:
            logger.error(f"Error getting listings for chat {chat_id}: {e}")
            return []

    def mark_profile_sent(self, fb_id: str, chat_id: int, telegram_message_id: int) -> None:
        """Mark a listing as sent for a specific chat profile."""
        query = """
            UPDATE listing_profiles
            SET sent_at = NOW(), telegram_message_id = %s
            WHERE fb_id = %s AND chat_id = %s
        """
        try:
            self.cursor.execute(query, (telegram_message_id, fb_id, chat_id))
            self.conn.commit()
        except Exception as e:
            logger.error(f"Error marking profile sent ({fb_id}, {chat_id}): {e}")
            self.conn.rollback()

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
