import json
import pandas as pd
import os
import logging
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)

# Load configuration from environment variables with defaults
RAW_DIR = os.getenv("RAW_DIR", "output/raw")
DATA_FILE = os.path.join(RAW_DIR, "pokemon_data.json")
HABITAT_DATA_FILE = os.path.join(RAW_DIR, "habitat_data.json")


def load_json_file(filepath: str, resource_name: str) -> List[Dict]:
    """
    Load and validate a JSON file.
    
    Args:
        filepath: Path to the JSON file
        resource_name: Name of the resource (for logging)
        
    Returns:
        List of dictionaries from the JSON file
    """
    try:
        with open(filepath, "r", encoding="utf-8") as file:
            data = json.load(file)
    except (OSError, IOError) as err:
        logger.error(f"Failed to open {resource_name} file: {err}")
        raise
    except json.JSONDecodeError as err:
        logger.error(f"{resource_name} file is not valid JSON: {err}")
        raise

    if not isinstance(data, list):
        logger.error(f"{resource_name} file structure is invalid: expected a list")
        raise ValueError(f"Invalid {resource_name} data format")
    
    return data


def extract_pokemon_abilities(abilities_list: List[Dict]) -> Dict[str, Any]:
    """
    Extract and organize Pokémon abilities from raw data.
    
    Args:
        abilities_list: List of ability dictionaries from API
        
    Returns:
        Dictionary with organized ability data
    """
    abilities = {
        "ability_1": None,
        "ability_1_is_hidden": None,
        "ability_2": None,
        "ability_2_is_hidden": None,
        "ability_3": None,
        "ability_3_is_hidden": None
    }

    for ability in abilities_list:
        slot = ability.get("slot")
        ability_name = ability.get("ability", {}).get("name")
        is_hidden = ability.get("is_hidden")
        
        if slot == 1:
            abilities["ability_1"] = ability_name
            abilities["ability_1_is_hidden"] = is_hidden
        elif slot == 2:
            abilities["ability_2"] = ability_name
            abilities["ability_2_is_hidden"] = is_hidden
        elif slot == 3:
            abilities["ability_3"] = ability_name
            abilities["ability_3_is_hidden"] = is_hidden
    
    return abilities


def extract_pokemon_types(types_list: List[Dict]) -> Dict[str, Optional[str]]:
    """
    Extract and organize Pokémon types from raw data.
    
    Args:
        types_list: List of type dictionaries from API
        
    Returns:
        Dictionary with organized type data
    """
    types = {
        "type_1": None,
        "type_2": None
    }

    for type_entry in types_list:
        slot = type_entry.get("slot")
        type_name = type_entry.get("type", {}).get("name")
        
        if slot == 1:
            types["type_1"] = type_name
        elif slot == 2:
            types["type_2"] = type_name
    
    return types


def extract_pokemon_stats(stats_list: List[Dict]) -> Dict[str, Optional[int]]:
    """
    Extract and organize Pokémon stats from raw data.
    
    Args:
        stats_list: List of stat dictionaries from API
        
    Returns:
        Dictionary with organized stat data
    """
    stats = {
        "hp": None,
        "attack": None,
        "defense": None,
        "special_attack": None,
        "special_defense": None,
        "speed": None
    }

    stat_mapping = {
        "hp": "hp",
        "attack": "attack",
        "defense": "defense",
        "special-attack": "special_attack",
        "special-defense": "special_defense",
        "speed": "speed"
    }

    for stat in stats_list:
        stat_name = stat.get("stat", {}).get("name")
        base_stat = stat.get("base_stat")
        
        if stat_name in stat_mapping:
            stats[stat_mapping[stat_name]] = base_stat
    
    return stats


def create_pokemon_tables() -> pd.DataFrame:
    """
    Read raw Pokémon JSON data and convert to structured DataFrame.
    
    Returns:
        DataFrame containing transformed Pokémon data
    """
    logger.info("Transforming Pokémon data...")
    
    data = load_json_file(DATA_FILE, "Pokémon data")
    pokemon_rows = []
    total_pokemon = len(data)
    errors = 0

    for idx, entry in enumerate(data, start=1):
        try:
            # Extract nested data structures
            abilities = extract_pokemon_abilities(entry.get("abilities", []))
            types = extract_pokemon_types(entry.get("types", []))
            stats = extract_pokemon_stats(entry.get("stats", []))

            # Build the row
            pokemon_row = {
                "id": entry.get("id"),
                "name": entry.get("name"),
                "species": entry.get("species", {}).get("name"),
                "base_experience": entry.get("base_experience"),
                **abilities,
                **types,
                **stats
            }
            
            pokemon_rows.append(pokemon_row)
            
            if idx % 100 == 0:
                logger.debug(f"Transformed {idx}/{total_pokemon} Pokémon")
                
        except KeyError as err:
            logger.warning(f"Missing key in Pokémon entry {idx}: {err}")
            errors += 1
            continue
        except Exception as err:
            pokemon_name = entry.get('name', 'unknown')
            logger.warning(f"Unexpected error processing Pokémon '{pokemon_name}': {err}")
            errors += 1
            continue

    try:
        pokemon_df = pd.DataFrame(pokemon_rows)

        logger.info(f"Successfully transformed {len(pokemon_rows)}/{total_pokemon} Pokémon into DataFrame")
        
        if errors > 0:
            logger.warning(f"Encountered {errors} errors during transformation")
        
        return pokemon_df
        
    except Exception as err:
        logger.error(f"Failed to create Pokémon DataFrame: {err}")
        raise


def create_habitat_tables() -> pd.DataFrame:
    """
    Read raw habitat JSON data and convert to structured DataFrame.
    
    Returns:
        DataFrame containing transformed habitat data
    """
    logger.info("Transforming Pokémon habitat data...")
    
    data = load_json_file(HABITAT_DATA_FILE, "Pokémon habitat data")
    habitat_rows = []
    total_habitats = len(data)
    errors = 0

    for idx, entry in enumerate(data, start=1):
        try:
            habitat_id = entry.get("id")
            habitat_name = entry.get("name")
            pokemon_species_list = entry.get("pokemon_species", [])
            
            # Create one row per Pokémon species in the habitat
            for species in pokemon_species_list:
                habitat_rows.append({
                    "habitat_id": habitat_id,
                    "habitat_name": habitat_name,
                    "pokemon_species": species.get("name")
                })
            
            logger.debug(f"Transformed habitat '{habitat_name}' ({idx}/{total_habitats})")
            
        except KeyError as err:
            logger.warning(f"Missing key in habitat entry {idx}: {err}")
            errors += 1
            continue
        except Exception as err:
            habitat_name = entry.get('name', 'unknown')
            logger.warning(f"Unexpected error processing habitat '{habitat_name}': {err}")
            errors += 1
            continue

    try:
        habitat_df = pd.DataFrame(habitat_rows)
        logger.info(f"Successfully transformed {len(habitat_rows)} habitat-species relationships from {total_habitats} habitats")
        
        if errors > 0:
            logger.warning(f"Encountered {errors} errors during habitat transformation")
        
        return habitat_df
        
    except Exception as err:
        logger.error(f"Failed to create habitat DataFrame: {err}")
        raise