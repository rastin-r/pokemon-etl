-- ============================================================================
-- Pokémon Database Schema
-- ============================================================================
-- This schema defines the structure for storing Pokémon and habitat data
-- extracted from PokéAPI (https://pokeapi.co)
--
-- Tables:
--   1. pokemon: Core Pokémon data including stats, types, and abilities
--   2. habitat: Pokémon species and their natural habitat
-- ============================================================================

-- ============================================================================
-- Table: pokemon
-- ============================================================================
-- Stores comprehensive Pokémon data including base stats, types, and abilities
-- ============================================================================

CREATE TABLE IF NOT EXISTS pokemon (
    -- Primary identifier
    id INTEGER PRIMARY KEY,
    
    -- Basic information
    name TEXT NOT NULL Unique,
    species TEXT NOT NULL,
    base_experience INTEGER,
    
    -- Abilities (up to 3 per Pokémon)
    ability_1 TEXT,
    ability_1_is_hidden BOOLEAN,
    ability_2 TEXT,
    ability_2_is_hidden BOOLEAN,
    ability_3 TEXT,
    ability_3_is_hidden BOOLEAN,
    
    -- Types (primary and secondary)
    type_1 TEXT,
    type_2 TEXT,
    
    -- Base stats
    hp INTEGER CHECK (hp >= 0),
    attack INTEGER CHECK (attack >= 0),
    defense INTEGER CHECK (defense >= 0),
    special_attack INTEGER CHECK (special_attack >= 0),
    special_defense INTEGER CHECK (special_defense >= 0),
    speed INTEGER CHECK (speed >= 0)
);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_pokemon_name ON pokemon(name);
CREATE INDEX IF NOT EXISTS idx_pokemon_type_1 ON pokemon(type_1);
CREATE INDEX IF NOT EXISTS idx_pokemon_type_2 ON pokemon(type_2);
CREATE INDEX IF NOT EXISTS idx_pokemon_species ON pokemon(species);

-- ============================================================================
-- Table: habitat
-- ============================================================================
-- Stores the relationship between Pokémon species and their primary habitat
-- ============================================================================

CREATE TABLE IF NOT EXISTS habitat (
    -- Habitat information
    habitat_id INTEGER NOT NULL,
    habitat_name TEXT NOT NULL,
    
    -- Pokémon species in this habitat
    pokemon_species TEXT PRIMARY KEY

    -- -- Foreign key constraint to ensure referential integrity
    -- CONSTRAINT fk_pokemon_species 
    --     FOREIGN KEY (pokemon_species) 
    --     REFERENCES pokemon(species)
    --     ON DELETE CASCADE
    --     ON UPDATE CASCADE
);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_habitat_name ON habitat(habitat_name);
CREATE INDEX IF NOT EXISTS idx_habitat_species ON habitat(pokemon_species);

-- ============================================================================
-- Comments
-- ============================================================================

COMMENT ON TABLE pokemon IS 'Core Pokémon data including stats, types, and abilities';
COMMENT ON TABLE habitat IS 'Relationship between Pokémon species and their natural habitat (supports multiple per species just in case)';

COMMENT ON COLUMN pokemon.id IS 'Unique Pokémon ID from PokéAPI';
COMMENT ON COLUMN pokemon.name IS 'Pokémon name (lowercase, hyphenated)';
COMMENT ON COLUMN pokemon.species IS 'Pokémon species name (unique identifier for FK relationship)';
COMMENT ON COLUMN pokemon.base_experience IS 'Base experience gained from defeating this Pokémon';
COMMENT ON COLUMN pokemon.ability_1_is_hidden IS 'Whether ability 1 is a hidden ability';
COMMENT ON COLUMN pokemon.type_1 IS 'Primary type (e.g., fire, water, grass)';
COMMENT ON COLUMN pokemon.type_2 IS 'Secondary type (NULL if mono-type)';

COMMENT ON COLUMN habitat.habitat_id IS 'Unique habitat identifier from PokéAPI';
COMMENT ON COLUMN habitat.habitat_name IS 'Name of the habitat (e.g., cave, forest, sea)';
COMMENT ON COLUMN habitat.pokemon_species IS 'Species name found in this habitat';

-- ============================================================================
-- End of schema
-- ============================================================================