"""
Database seeding script entry point.
====================================
Populates dimension tables from CSV data.
"""

import os
import sys
import logging
from dotenv import load_dotenv

# Add app to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.core.seeder import DatabaseSeeder


def setup_logging() -> None:
    """Configure logging for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler()
        ]
    )


def main() -> None:
    """Main entry point for database seeding."""
    # Load environment variables
    load_dotenv()
    
    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)
    
    # Get configuration from environment
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_name = os.getenv("DB_NAME")
    db_host = os.getenv("DB_HOST")
    csv_path = os.getenv("CSV_PATH", "data/nigerian_cities.csv")
    
    # Validate configuration
    if not all([db_user, db_password, db_name, db_host]):
        logger.error("Database environment variables not set")
        sys.exit(1)
    
    try:
        # Initialize and run seeder
        with DatabaseSeeder(
            db_host=db_host,
            db_name=db_name,
            db_user=db_user,
            db_password=db_password,
            csv_path=csv_path
        ) as seeder:
            logger.info("Starting database seeding...")
            seeder.seed()
            logger.info("Database seeding completed successfully")
            
    except FileNotFoundError as e:
        logger.error(f"CSV file not found: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Seeding failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
