import requests
import json
import os
import logging

logger = logging.getLogger(__name__)

RAW_DIR = "raw"
POKEMON_LIST_FILE = os.path.join(RAW_DIR, "pokemon_list.json")
POKEMON_DATA_FILE = os.path.join(RAW_DIR, "pokemon_data.json")


def get_pokemon_list():
    """Fetch the list of all Pokémon and save to JSON."""
    try:
        os.makedirs(RAW_DIR, exist_ok=True)
    except OSError as err:
        logger.error(f"Failed to create directory {RAW_DIR}: {err}")
        raise

    base_url = "https://pokeapi.co/api/v2/pokemon/"

    try:
        response = requests.get(base_url, timeout=10)
        response.raise_for_status()
        all_data = response.json().copy()
    except requests.exceptions.RequestException as err:
        logger.error(f"Failed initial Pokémon list request: {err}")
        raise
    except json.JSONDecodeError as err:
        logger.error(f"Failed to parse JSON from initial response: {err}")
        raise

    try:
        while all_data.get("next") is not None:
            next_url = all_data["next"]
            try:
                next_response = requests.get(next_url, timeout=10)
                next_response.raise_for_status()
                next_data = next_response.json()
                all_data["results"].extend(next_data["results"])
                all_data["next"] = next_data["next"]
            except requests.exceptions.RequestException as err:
                logger.error(f"Failed fetching next Pokémon page: {err}")
                break
            except json.JSONDecodeError as err:
                logger.error(f"Failed parsing next Pokémon page: {err}")
                break

        try:
            with open(POKEMON_LIST_FILE, "w") as file:
                json.dump(all_data, file, indent=4)
            logger.info(f"Pokémon list saved to {POKEMON_LIST_FILE}")
        except (OSError, IOError) as err:
            logger.error(f"Failed to write Pokémon list to file: {err}")
            raise

    except Exception as err:
        logger.error(f"Unexpected error in get_pokemon_list: {err}")
        raise


def fetch_pokemon_details():
    """Fetch details for each Pokémon from the list and save to JSON."""
    try:
        with open(POKEMON_LIST_FILE, "r") as file:
            data = json.load(file)
    except (OSError, IOError) as err:
        logger.error(f"Failed to read Pokémon list file: {err}")
        raise
    except json.JSONDecodeError as err:
        logger.error(f"Pokémon list file is not valid JSON: {err}")
        raise

    all_pokemon = []

    try:
        for i, item in enumerate(data.get("results", []), start=1):
            try:
                response = requests.get(item["url"], timeout=10)
                response.raise_for_status()
                poke_json = response.json()
                all_pokemon.append(poke_json)
            except requests.exceptions.RequestException as err:
                logger.warning(f"Warning: Skipping Pokémon {item.get('name')} due to request error: {err}")
                continue
            except json.JSONDecodeError as err:
                logger.warning(f"Warning: Skipping Pokémon {item.get('name')} due to JSON parse error: {err}")
                continue

            if i % 50 == 0:
                logger.info(f"Fetched {i} Pokémon...")

        try:
            with open(POKEMON_DATA_FILE, "w") as file:
                json.dump(all_pokemon, file, indent=4)
            logger.info(f"Pokémon details saved to {POKEMON_DATA_FILE}")
        except (OSError, IOError) as err:
            logger.error(f"Failed to write Pokémon details file: {err}")
            raise

    except Exception as err:
        logger.error(f"Unexpected error in fetch_pokemon_details: {err}")
        raise
