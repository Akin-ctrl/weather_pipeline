"""
Main entry point for the weather pipeline.
==========================================
Starts the scheduled ETL pipeline.
"""

import os
import sys
import time
import logging
from dotenv import load_dotenv

# Add app to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.core.pipeline import WeatherPipeline


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
    """Main entry point for the weather pipeline."""
    # Load environment variables
    load_dotenv()
    
    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)
    
    # Get configuration from environment
    api_key = os.getenv("OPENWEATHER_API_KEY")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_name = os.getenv("DB_NAME")
    db_host = os.getenv("DB_HOST")
    interval_minutes = int(os.getenv("PIPELINE_INTERVAL_MINUTES", "10"))
    
    # Validate configuration
    if not api_key:
        logger.error("OPENWEATHER_API_KEY environment variable not set")
        sys.exit(1)
    
    if not all([db_user, db_password, db_name, db_host]):
        logger.error("Database environment variables not set")
        sys.exit(1)
    
    # Wait for database to be ready
    logger.info("Waiting for database to be ready...")
    time.sleep(10)
    
    try:
        # Initialize and run pipeline
        with WeatherPipeline(
            db_host=db_host,
            db_name=db_name,
            db_user=db_user,
            db_password=db_password,
            api_key=api_key,
            interval_minutes=interval_minutes
        ) as pipeline:
            logger.info("Starting weather pipeline...")
            pipeline.run_scheduled()
            
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
