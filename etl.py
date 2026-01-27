import os
import sys
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

from etl.extract import create_raw_folder, fetch_pokemon_list, fetch_pokemon_details, fetch_habitat_list, fetch_habitat_details
from etl.transform import create_pokemon_tables, create_habitat_tables
from etl.load import save_pokemon_to_csv, save_habitat_to_csv, save_pokemon_to_sql, save_habitat_to_sql, copy_schema_file

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(os.getenv("LOGS_DIR", "logs"), "etl.log"), mode='a')
    ]
)

logger = logging.getLogger(__name__)


def main():
    """
    Main ETL pipeline for Pokémon data.
    
    This script extracts data from PokéAPI, transforms it into structured tables,
    and loads it into CSV and SQL files locally.
    """
    logger.info("=" * 80)
    logger.info("Starting Pokémon ETL Pipeline (Local Mode)")
    logger.info("=" * 80)

    try:
        # ========== EXTRACT ==========
        logger.info("\n[1/3] EXTRACT - Fetching data from PokéAPI...")
        create_raw_folder()
        
        logger.info("  → Fetching Pokémon list...")
        fetch_pokemon_list()
        
        logger.info("  → Fetching Pokémon details...")
        fetch_pokemon_details()
        
        logger.info("  → Fetching habitat list...")
        fetch_habitat_list()
        
        logger.info("  → Fetching habitat details...")
        fetch_habitat_details()
        
        logger.info("✓ Extract phase completed successfully")

        # ========== TRANSFORM ==========
        logger.info("\n[2/3] TRANSFORM - Processing and structuring data...")
        
        logger.info("  → Transforming Pokémon data...")
        pokemon_df = create_pokemon_tables()
        
        logger.info("  → Transforming habitat data...")
        habitat_df = create_habitat_tables()
        
        logger.info(f"✓ Transform phase completed successfully")
        logger.info(f"  • Pokémon records: {len(pokemon_df)}")
        logger.info(f"  • Habitat relationships: {len(habitat_df)}")

        # ========== LOAD ==========
        logger.info("\n[3/3] LOAD - Saving data to local files...")
        
        logger.info("  → Saving CSV files...")
        save_pokemon_to_csv(pokemon_df)
        save_habitat_to_csv(habitat_df)
        
        logger.info("  → Generating SQL files...")
        save_pokemon_to_sql(pokemon_df)
        save_habitat_to_sql(habitat_df)
        copy_schema_file()
        
        logger.info("✓ Load phase completed successfully")

        # ========== SUMMARY ==========
        logger.info("\n" + "=" * 80)
        logger.info("ETL Pipeline Completed Successfully! 🎉")
        logger.info("=" * 80)
        logger.info(f"Output locations:")
        logger.info(f"  • Raw JSON files: {os.getenv('RAW_DIR', 'output/raw')}")
        logger.info(f"  • CSV files: {os.getenv('CSV_DIR', 'output/csv')}")
        logger.info(f"  • SQL files: {os.getenv('SQL_DIR', 'output/sql')}")
        logger.info(f"  • Log file: {os.path.join(os.getenv('LOGS_DIR', 'logs'), 'etl.log')}")
        logger.info("=" * 80)

    except KeyboardInterrupt:
        logger.warning("\nETL Pipeline interrupted by user")
        sys.exit(130)  # Standard exit code for Ctrl+C
        
    except Exception as err:
        logger.error("\n" + "=" * 80)
        logger.error(f"ETL Pipeline FAILED: {err}", exc_info=True)
        logger.error("=" * 80)
        sys.exit(1)


if __name__ == "__main__":
    # Create logs directory if it doesn't exist
    os.makedirs(os.getenv("LOGS_DIR", "logs"), exist_ok=True)
    main()
