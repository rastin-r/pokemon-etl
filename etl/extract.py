import requests
import json
import os
import logging
from typing import Dict, List, Optional, Union

logger = logging.getLogger(__name__)

# Load configuration from environment variables with defaults
RAW_DIR = os.getenv("RAW_DIR", "output/raw")
POKEMON_API_BASE_URL = os.getenv("POKEMON_API_BASE_URL", "https://pokeapi.co/api/v2")
API_TIMEOUT = int(os.getenv("API_TIMEOUT", "10"))

# File paths
POKEMON_LIST_FILE = os.path.join(RAW_DIR, "pokemon_list.json")
POKEMON_DATA_FILE = os.path.join(RAW_DIR, "pokemon_data.json")
HABITAT_LIST_FILE = os.path.join(RAW_DIR, "habitat_list.json")
HABITAT_DATA_FILE = os.path.join(RAW_DIR, "habitat_data.json")


def create_raw_folder() -> None:
    """Create the raw data directory if it doesn't exist."""
    try:
        os.makedirs(RAW_DIR, exist_ok=True)
        logger.info(f"Raw data directory ready: {RAW_DIR}")
    except OSError as err:
        logger.error(f"Failed to create directory {RAW_DIR}: {err}")
        raise


def fetch_paginated_data(base_url: str, resource_name: str) -> Dict:
    """
    Fetch all paginated data from a PokéAPI endpoint.
    
    Args:
        base_url: The API endpoint URL
        resource_name: Name of the resource (for logging)
        
    Returns:
        Dictionary containing all results from paginated API calls
    """
    try:
        response = requests.get(base_url, timeout=API_TIMEOUT)
        response.raise_for_status()
        all_data = response.json().copy()
        logger.info(f"Initial {resource_name} fetch successful")
    except requests.exceptions.RequestException as err:
        logger.error(f"Failed initial {resource_name} request: {err}")
        raise
    except json.JSONDecodeError as err:
        logger.error(f"Failed to parse JSON from initial {resource_name} response: {err}")
        raise

    try:
        page_count = 1
        while all_data.get("next") is not None:
            next_url = all_data["next"]
            try:
                next_response = requests.get(next_url, timeout=API_TIMEOUT)
                next_response.raise_for_status()
                next_data = next_response.json()
                all_data["results"].extend(next_data["results"])
                all_data["next"] = next_data["next"]
                page_count += 1
                logger.debug(f"Fetched {resource_name} page {page_count}")
            except requests.exceptions.RequestException as err:
                logger.error(f"Failed fetching {resource_name} page {page_count}: {err}")
                break
            except json.JSONDecodeError as err:
                logger.error(f"Failed parsing {resource_name} page {page_count}: {err}")
                break
        
        logger.info(f"Successfully fetched all {resource_name} data ({page_count} pages)")
        return all_data
    except Exception as err:
        logger.error(f"Unexpected error in fetch_paginated_data for {resource_name}: {err}")
        raise


def save_json_data(data: Union[Dict, List], filepath: str, resource_name: str) -> None:
    """
    Save data to a JSON file.
    
    Args:
        data: Data to save (dict or list)
        filepath: Path to save the file
        resource_name: Name of the resource (for logging)
    """
    try:
        with open(filepath, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=2)
        logger.info(f"{resource_name} saved to {filepath}")
    except (OSError, IOError) as err:
        logger.error(f"Failed to write {resource_name} to file: {err}")
        raise


def fetch_pokemon_list() -> None:
    """Fetch the complete list of all Pokémon and save to JSON."""
    logger.info("Fetching Pokémon list...")
    base_url = f"{POKEMON_API_BASE_URL}/pokemon/"
    
    try:
        all_data = fetch_paginated_data(base_url, "Pokémon list")
        save_json_data(all_data, POKEMON_LIST_FILE, "Pokémon list")
        logger.info(f"Total Pokémon in list: {len(all_data.get('results', []))}")
    except Exception as err:
        logger.error(f"Failed to fetch Pokémon list: {err}")
        raise


def fetch_pokemon_details() -> None:
    """Fetch detailed information for each Pokémon from the list and save to JSON."""
    logger.info("Fetching Pokémon details...")
    
    try:
        with open(POKEMON_LIST_FILE, "r", encoding="utf-8") as file:
            data = json.load(file)
    except (OSError, IOError) as err:
        logger.error(f"Failed to read Pokémon list file: {err}")
        raise
    except json.JSONDecodeError as err:
        logger.error(f"Pokémon list file is not valid JSON: {err}")
        raise

    all_pokemon = []
    pokemon_list = data.get("results", [])
    total_pokemon = len(pokemon_list)
    
    if total_pokemon == 0:
        logger.warning("No Pokémon found in list file")
        return

    try:
        for i, item in enumerate(pokemon_list, start=1):
            pokemon_name = item.get('name', 'unknown')
            
            try:
                response = requests.get(item["url"], timeout=API_TIMEOUT)
                response.raise_for_status()
                poke_json = response.json()
                all_pokemon.append(poke_json)
                
                if i % 50 == 0 or i == total_pokemon:
                    logger.info(f"Fetched {i}/{total_pokemon} Pokémon details ({i/total_pokemon*100:.1f}%)")
                    
            except requests.exceptions.RequestException as err:
                logger.warning(f"Skipping Pokémon '{pokemon_name}' due to request error: {err}")
                continue
            except json.JSONDecodeError as err:
                logger.warning(f"Skipping Pokémon '{pokemon_name}' due to JSON parse error: {err}")
                continue
            except KeyError as err:
                logger.warning(f"Skipping Pokémon entry due to missing URL key: {err}")
                continue

        save_json_data(all_pokemon, POKEMON_DATA_FILE, "Pokémon details")
        logger.info(f"Successfully fetched details for {len(all_pokemon)}/{total_pokemon} Pokémon")
        
    except Exception as err:
        logger.error(f"Unexpected error in fetch_pokemon_details: {err}")
        raise


def fetch_habitat_list() -> None:
    """Fetch the complete list of all Pokémon habitats and save to JSON."""
    logger.info("Fetching Pokémon habitat list...")
    base_url = f"{POKEMON_API_BASE_URL}/pokemon-habitat/"
    
    try:
        all_data = fetch_paginated_data(base_url, "Pokémon habitat list")
        save_json_data(all_data, HABITAT_LIST_FILE, "Pokémon habitat list")
        logger.info(f"Total habitats in list: {len(all_data.get('results', []))}")
    except Exception as err:
        logger.error(f"Failed to fetch habitat list: {err}")
        raise


def fetch_habitat_details() -> None:
    """Fetch detailed information for each habitat from the list and save to JSON."""
    logger.info("Fetching Pokémon habitat details...")
    
    try:
        with open(HABITAT_LIST_FILE, "r", encoding="utf-8") as file:
            data = json.load(file)
    except (OSError, IOError) as err:
        logger.error(f"Failed to read Pokémon habitat list file: {err}")
        raise
    except json.JSONDecodeError as err:
        logger.error(f"Pokémon habitat list file is not valid JSON: {err}")
        raise

    all_pokemon_habitat = []
    habitat_list = data.get("results", [])
    total_habitats = len(habitat_list)
    
    if total_habitats == 0:
        logger.warning("No habitats found in list file")
        return

    try:
        for i, item in enumerate(habitat_list, start=1):
            habitat_name = item.get('name', 'unknown')
            
            try:
                response = requests.get(item["url"], timeout=API_TIMEOUT)
                response.raise_for_status()
                poke_habitat_json = response.json()
                all_pokemon_habitat.append(poke_habitat_json)
                logger.info(f"Fetched habitat '{habitat_name}' ({i}/{total_habitats})")
                
            except requests.exceptions.RequestException as err:
                logger.warning(f"Skipping habitat '{habitat_name}' due to request error: {err}")
                continue
            except json.JSONDecodeError as err:
                logger.warning(f"Skipping habitat '{habitat_name}' due to JSON parse error: {err}")
                continue
            except KeyError as err:
                logger.warning(f"Skipping habitat entry due to missing URL key: {err}")
                continue

        save_json_data(all_pokemon_habitat, HABITAT_DATA_FILE, "Pokémon habitat details")
        logger.info(f"Successfully fetched details for {len(all_pokemon_habitat)}/{total_habitats} habitats")
        
    except Exception as err:
        logger.error(f"Unexpected error in fetch_habitat_details: {err}")
        raise