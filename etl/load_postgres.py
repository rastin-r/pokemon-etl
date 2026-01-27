import os
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
from typing import Optional

logger = logging.getLogger(__name__)

# Load database URL from environment variable
DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://pokemon:pokemon@postgres:5432/pokemon"
)


def get_engine() -> Engine:
    """
    Create and return a SQLAlchemy engine for database connection.
    
    Returns:
        SQLAlchemy Engine object
    """
    try:
        engine = create_engine(DB_URL, pool_pre_ping=True)
        logger.info("Database engine created successfully")
        return engine
    except SQLAlchemyError as err:
        logger.error(f"Failed to create database engine: {err}")
        raise
    except Exception as err:
        logger.error(f"Unexpected error creating database engine: {err}")
        raise


def test_connection(engine: Engine) -> bool:
    """
    Test the database connection.
    
    Args:
        engine: SQLAlchemy Engine object
        
    Returns:
        True if connection is successful, False otherwise
    """
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Database connection test successful")
        return True
    except SQLAlchemyError as err:
        logger.error(f"Database connection test failed: {err}")
        return False
    except Exception as err:
        logger.error(f"Unexpected error testing connection: {err}")
        return False


def run_sql_file(engine: Engine, filename: str, sql_dir: str = "sql") -> None:
    """
    Execute SQL statements from a file.
    
    Args:
        engine: SQLAlchemy Engine object
        filename: Name of the SQL file to execute
        sql_dir: Directory containing SQL files (default: "sql")
    """
    filepath = os.path.join(sql_dir, filename)
    
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            sql = f.read()
        
        logger.info(f"Executing SQL file: {filepath}")
        
        # Remove all single-line comments (lines starting with --)
        lines = sql.split('\n')
        cleaned_lines = []
        for line in lines:
            # Remove comments but keep the rest of the line if it has SQL
            if '--' in line:
                line = line.split('--')[0]
            if line.strip():
                cleaned_lines.append(line)
        
        cleaned_sql = '\n'.join(cleaned_lines)
        
        # Remove multi-line comments (/* ... */)
        import re
        cleaned_sql = re.sub(r'/\*.*?\*/', '', cleaned_sql, flags=re.DOTALL)
        
        # Split by semicolon and filter out empty statements
        statements = [stmt.strip() for stmt in cleaned_sql.split(';') if stmt.strip()]
        
        logger.info(f"Found {len(statements)} SQL statements to execute")
        
        with engine.begin() as conn:
            for idx, statement in enumerate(statements, start=1):
                try:
                    logger.debug(f"Executing statement {idx}/{len(statements)}")
                    conn.execute(text(statement))
                except SQLAlchemyError as err:
                    logger.error(f"Error executing statement {idx}: {err}")
                    logger.error(f"Statement was: {statement[:200]}...")
                    raise
        
        logger.info(f"Successfully executed SQL file: {filepath}")
        
    except (OSError, IOError) as err:
        logger.error(f"Failed to read SQL file {filepath}: {err}")
        raise
    except SQLAlchemyError as err:
        logger.error(f"Database error executing SQL file {filepath}: {err}")
        raise
    except Exception as err:
        logger.error(f"Unexpected error executing SQL file {filepath}: {err}")
        raise
    
def load_dataframe(engine: Engine, df: pd.DataFrame, table_name: str, 
                   if_exists: str = "replace", chunksize: Optional[int] = 1000) -> None:
    """
    Load a pandas DataFrame into a database table.
    
    Args:
        engine: SQLAlchemy Engine object
        df: DataFrame to load
        table_name: Name of the target database table
        if_exists: How to behave if table exists ('fail', 'replace', 'append')
        chunksize: Number of rows to write at a time (None for all at once)
    """
    try:
        if df.empty:
            logger.warning(f"DataFrame for table '{table_name}' is empty. Skipping load.")
            return
        
        logger.info(f"Loading {len(df)} rows into table '{table_name}'...")
        
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists=if_exists,
            index=False,
            chunksize=chunksize,
            method='multi'  # Use multi-row INSERT for better performance
        )
        
        logger.info(f"Successfully loaded {len(df)} rows into table '{table_name}'")
        
    except SQLAlchemyError as err:
        logger.error(f"Database error loading DataFrame into '{table_name}': {err}")
        raise
    except ValueError as err:
        logger.error(f"Value error loading DataFrame into '{table_name}': {err}")
        raise
    except Exception as err:
        logger.error(f"Unexpected error loading DataFrame into '{table_name}': {err}")
        raise


def verify_table_data(engine: Engine, table_name: str) -> int:
    """
    Verify that data was loaded into a table by counting rows.
    
    Args:
        engine: SQLAlchemy Engine object
        table_name: Name of the table to verify
        
    Returns:
        Number of rows in the table
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            row_count = result.scalar()
        
        logger.info(f"Table '{table_name}' contains {row_count} rows")
        return row_count
        
    except SQLAlchemyError as err:
        logger.error(f"Error verifying table '{table_name}': {err}")
        raise
    except Exception as err:
        logger.error(f"Unexpected error verifying table '{table_name}': {err}")
        raise


def load_pokemon_data_to_postgres(pokemon_df: pd.DataFrame, habitat_df: pd.DataFrame) -> None:
    """
    Complete workflow to load Pokémon data into PostgreSQL database.
    
    Args:
        pokemon_df: DataFrame containing Pokémon data
        habitat_df: DataFrame containing habitat data
    """
    try:
        # Create database engine
        engine = get_engine()
        
        # Test connection
        if not test_connection(engine):
            raise ConnectionError("Unable to connect to database")
        
        # Run schema file to create tables
        logger.info("Creating database schema...")
        run_sql_file(engine, "schema.sql")
        
        # Load Pokémon data
        load_dataframe(engine, pokemon_df, "pokemon")
        pokemon_count = verify_table_data(engine, "pokemon")
        
        # Load habitat data
        load_dataframe(engine, habitat_df, "habitat")
        habitat_count = verify_table_data(engine, "habitat")
        
        logger.info(f"Database load complete: {pokemon_count} Pokémon, {habitat_count} habitat relationships")
        
    except Exception as err:
        logger.error(f"Failed to load data to PostgreSQL: {err}")
        raise