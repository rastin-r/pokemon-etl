import json
import pandas as pd
import os
import logging

logger = logging.getLogger(__name__)

RAW_DIR = "raw"
DATA_FILE = os.path.join(RAW_DIR, "pokemon_data.json")
TRANSFORMED_DIR = "transformed"


def create_tables():
    """Read raw JSON and convert to structured tables (DataFrames)."""
    try:
        os.makedirs(TRANSFORMED_DIR, exist_ok=True)
    except OSError as err:
        logger.error(f"Failed to create directory {TRANSFORMED_DIR}: {err}")
        raise

    try:
        with open(DATA_FILE, "r") as file:
            data = json.load(file)
    except (OSError, IOError) as err:
        logger.error(f"Failed to open Pokémon data file: {err}")
        raise
    except json.JSONDecodeError as err:
        logger.error(f"Pokémon data file is not valid JSON: {err}")
        raise

    # Validate that data is a list
    if not isinstance(data, list):
        logger.error("Pokémon data file structure is invalid: expected a list of Pokémon.")
        raise ValueError("Invalid Pokémon data format")

    pokemon_rows, types_rows, abilities_rows = [], [], []

    try:
        for entry in data:
            try:
                pokemon_rows.append({
                    "id": entry["id"],
                    "name": entry["name"],
                    "height": entry["height"],
                    "weight": entry["weight"],
                    "base_experience": entry.get("base_experience", None)
                })

                for t in entry.get("types", []):
                    types_rows.append({
                        "pokemon_id": entry["id"],
                        "type_name": t["type"]["name"]
                    })

                for a in entry.get("abilities", []):
                    abilities_rows.append({
                        "pokemon_id": entry["id"],
                        "ability_name": a["ability"]["name"],
                        "is_hidden": a.get("is_hidden", False),
                        "slot": a.get("slot", None)
                    })
            except KeyError as err:
                logger.warning(f"Warning: Missing key in Pokémon entry: {err}")
                continue
            except Exception as err:
                logger.warning(f"Warning: Unexpected error processing Pokémon {entry.get('name', 'unknown')}: {err}")
                continue

        # Convert to DataFrames
        try:
            pokemon_df = pd.DataFrame(pokemon_rows)
            types_df = pd.DataFrame(types_rows)
            abilities_df = pd.DataFrame(abilities_rows)
        except Exception as err:
            logger.error(f"Failed to create DataFrames: {err}")
            raise

        logger.info("Successfully transformed Pokémon data into tables")
        return pokemon_df, types_df, abilities_df

    except Exception as err:
        logger.error(f"Unexpected error in create_tables: {err}")
        raise
