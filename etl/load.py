import os
import logging
import pandas as pd
from typing import Optional

logger = logging.getLogger(__name__)

# Load configuration from environment variables with defaults
CSV_DIR = os.getenv("CSV_DIR", "output/csv")
SQL_DIR = os.getenv("SQL_DIR", "output/sql")


def create_output_directories() -> None:
    """Create output directories for CSV and SQL files if they don't exist."""
    try:
        os.makedirs(CSV_DIR, exist_ok=True)
        os.makedirs(SQL_DIR, exist_ok=True)
        logger.info(f"Output directories ready: {CSV_DIR}, {SQL_DIR}")
    except OSError as err:
        logger.error(f"Failed to create output directories: {err}")
        raise


def save_pokemon_to_csv(pokemon_df: pd.DataFrame) -> None:
    """
    Save Pokémon DataFrame to CSV file.
    
    Args:
        pokemon_df: DataFrame containing Pokémon data
    """
    create_output_directories()
    
    try:
        pokemon_path = os.path.join(CSV_DIR, "pokemon.csv")
        pokemon_df.to_csv(pokemon_path, index=False, encoding="utf-8")
        logger.info(f"Saved Pokémon data to {pokemon_path} ({len(pokemon_df)} rows)")
    except (OSError, IOError, ValueError) as err:
        logger.error(f"Failed to save Pokémon CSV: {err}")
        raise
    except Exception as err:
        logger.error(f"Unexpected error saving Pokémon CSV: {err}")
        raise


def save_habitat_to_csv(habitat_df: pd.DataFrame) -> None:
    """
    Save habitat DataFrame to CSV file.
    
    Args:
        habitat_df: DataFrame containing habitat data
    """
    create_output_directories()
    
    try:
        habitat_path = os.path.join(CSV_DIR, "habitat.csv")
        habitat_df.to_csv(habitat_path, index=False, encoding="utf-8")
        logger.info(f"Saved habitat data to {habitat_path} ({len(habitat_df)} rows)")
    except (OSError, IOError, ValueError) as err:
        logger.error(f"Failed to save habitat CSV: {err}")
        raise
    except Exception as err:
        logger.error(f"Unexpected error saving habitat CSV: {err}")
        raise


def generate_insert_statements(df: pd.DataFrame, table_name: str, batch_size: int = 100) -> str:
    """
    Generate SQL INSERT statements from a DataFrame.
    
    Args:
        df: DataFrame to convert to SQL
        table_name: Name of the database table
        batch_size: Number of rows per INSERT statement
        
    Returns:
        String containing SQL INSERT statements
    """
    if df.empty:
        logger.warning(f"DataFrame for table '{table_name}' is empty")
        return f"-- No data to insert for table {table_name}\n"
    
    columns = df.columns.tolist()
    column_names = ", ".join(columns)
    sql_statements = []
    
    sql_statements.append(f"-- INSERT statements for table: {table_name}")
    sql_statements.append(f"-- Total rows: {len(df)}\n")
    
    # Process in batches
    for batch_start in range(0, len(df), batch_size):
        batch_end = min(batch_start + batch_size, len(df))
        batch_df = df.iloc[batch_start:batch_end]
        
        values_list = []
        for _, row in batch_df.iterrows():
            # Format each value properly (escape strings, handle nulls)
            formatted_values = []
            for val in row:
                if pd.isna(val) or val is None:
                    formatted_values.append("NULL")
                elif isinstance(val, (int, float)):
                    formatted_values.append(str(val))
                elif isinstance(val, bool):
                    formatted_values.append("TRUE" if val else "FALSE")
                else:
                    # Escape single quotes in strings
                    escaped_val = str(val).replace("'", "''")
                    formatted_values.append(f"'{escaped_val}'")
            
            values_list.append(f"({', '.join(formatted_values)})")
        
        insert_stmt = f"INSERT INTO {table_name} ({column_names})\nVALUES\n"
        insert_stmt += ",\n".join(values_list)
        insert_stmt += ";\n\n"
        sql_statements.append(insert_stmt)
    
    return "\n".join(sql_statements)


def save_pokemon_to_sql(pokemon_df: pd.DataFrame) -> None:
    """
    Generate and save SQL INSERT statements for Pokémon data.
    
    Args:
        pokemon_df: DataFrame containing Pokémon data
    """
    create_output_directories()
    
    try:
        sql_path = os.path.join(SQL_DIR, "pokemon_inserts.sql")
        
        logger.info("Generating SQL INSERT statements for Pokémon...")
        sql_content = generate_insert_statements(pokemon_df, "pokemon", batch_size=50)
        
        with open(sql_path, "w", encoding="utf-8") as f:
            f.write(sql_content)
        
        logger.info(f"Saved Pokémon SQL INSERT statements to {sql_path}")
    except (OSError, IOError) as err:
        logger.error(f"Failed to save Pokémon SQL file: {err}")
        raise
    except Exception as err:
        logger.error(f"Unexpected error saving Pokémon SQL: {err}")
        raise


def save_habitat_to_sql(habitat_df: pd.DataFrame) -> None:
    """
    Generate and save SQL INSERT statements for habitat data.
    
    Args:
        habitat_df: DataFrame containing habitat data
    """
    create_output_directories()
    
    try:
        sql_path = os.path.join(SQL_DIR, "habitat_inserts.sql")
        
        logger.info("Generating SQL INSERT statements for habitats...")
        sql_content = generate_insert_statements(habitat_df, "habitat", batch_size=100)
        
        with open(sql_path, "w", encoding="utf-8") as f:
            f.write(sql_content)
        
        logger.info(f"Saved habitat SQL INSERT statements to {sql_path}")
    except (OSError, IOError) as err:
        logger.error(f"Failed to save habitat SQL file: {err}")
        raise
    except Exception as err:
        logger.error(f"Unexpected error saving habitat SQL: {err}")
        raise


def copy_schema_file() -> None:
    """
    Copy the schema.sql file to the SQL output directory for reference.
    """
    create_output_directories()
    
    try:
        schema_source = os.path.join("sql", "schema.sql")
        schema_dest = os.path.join(SQL_DIR, "schema.sql")
        
        if os.path.exists(schema_source):
            with open(schema_source, "r", encoding="utf-8") as f:
                schema_content = f.read()
            
            with open(schema_dest, "w", encoding="utf-8") as f:
                f.write(schema_content)
            
            logger.info(f"Copied schema file to {schema_dest}")
        else:
            logger.warning(f"Schema file not found at {schema_source}")
    except (OSError, IOError) as err:
        logger.error(f"Failed to copy schema file: {err}")
        raise
    except Exception as err:
        logger.error(f"Unexpected error copying schema file: {err}")
        raise