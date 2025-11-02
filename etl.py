from extract import get_pokemon_list, fetch_pokemon_details
from transform import create_tables
from load import save_to_csv
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def main():
    logger.info("Starting Pokémon ETL Pipeline...")

    try:
        logger.info("\n[1/3] Extracting data...")
        get_pokemon_list()
        fetch_pokemon_details()

        logger.info("\n[2/3] Transforming data...")
        pokemon_df, types_df, abilities_df = create_tables()

        logger.info("\n[3/3] Loading data...")
        save_to_csv(pokemon_df, types_df, abilities_df)

        logger.info("\nETL Pipeline Completed Successfully!")

    except Exception as err:
        logger.error(f"ETL Pipeline failed: {err}", exc_info=True)
        sys.exit(1)  # Exit with non-zero code for automation tools (CI/CD, cron, etc.)


if __name__ == "__main__":
    main()
