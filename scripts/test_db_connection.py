#!/usr/bin/env python3
"""
A simple script to test the database connection using the environment variables
provided inside the Docker container.
"""

import os
import sys
from pathlib import Path
import logging

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from database import Database

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("--- Attempting to connect to the database ---")
    try:
        # The Database class now reads env vars from the container's environment
        with Database() as db:
            # The __enter__ method calls self.connect()
            logger.info("✅ SUCCESS: Database connection established and closed successfully.")
        sys.exit(0) # Exit with success code
    except Exception as e:
        logger.error("❌ FAILED: Could not connect to the database.")
        logger.error(f"Error: {e}", exc_info=True)
        
        # Log the env vars it tried to use, for debugging
        logger.error(f"Host: {os.getenv('POSTGRES_HOST')}")
        logger.error(f"Port: {os.getenv('POSTGRES_PORT')}")
        logger.error(f"DB Name: {os.getenv('POSTGRES_DB')}")
        logger.error(f"User: {os.getenv('POSTGRES_USER')}")
        logger.error(f"Password is set: {'yes' if os.getenv('POSTGRES_PASSWORD') else 'no'}")
        
        sys.exit(1) # Exit with error code

if __name__ == '__main__':
    main()
