import os
import logging

logger = logging.getLogger(__name__)

OUTPUT_DIR = "data"


def save_to_csv(pokemon_df, types_df, abilities_df):
    """Save DataFrames to CSV files."""
    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
    except OSError as err:
        logger.error(f"Failed to create output directory '{OUTPUT_DIR}': {err}")
        raise

    try:
        pokemon_path = os.path.join(OUTPUT_DIR, "pokemon.csv")
        types_path = os.path.join(OUTPUT_DIR, "pokemon_types.csv")
        abilities_path = os.path.join(OUTPUT_DIR, "pokemon_abilities.csv")

        try:
            pokemon_df.to_csv(pokemon_path, index=False)
            logger.info(f"Saved Pokémon data to {pokemon_path}")
        except (OSError, IOError, ValueError) as err:
            logger.error(f"Failed to save Pokémon CSV: {err}")

        try:
            types_df.to_csv(types_path, index=False)
            logger.info(f"Saved Pokémon types to {types_path}")
        except (OSError, IOError, ValueError) as err:
            logger.error(f"Failed to save Pokémon types CSV: {err}")

        try:
            abilities_df.to_csv(abilities_path, index=False)
            logger.info(f"Saved Pokémon abilities to {abilities_path}")
        except (OSError, IOError, ValueError) as err:
            logger.error(f"Failed to save Pokémon abilities CSV: {err}")

        logger.info("CSV export process completed.")

    except Exception as err:
        logger.error(f"Unexpected error during CSV saving: {err}")
        raise
