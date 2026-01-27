import sys
import os
from sqlalchemy import text

# Add the airflow directory to Python path so it can find the etl module
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
sys.path.insert(0, '/opt/airflow')

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import logging

from etl.extract import create_raw_folder, fetch_pokemon_list, fetch_pokemon_details, fetch_habitat_list, fetch_habitat_details
from etl.transform import create_pokemon_tables, create_habitat_tables
from etl.load_postgres import get_engine, run_sql_file, load_dataframe, test_connection, verify_table_data

logger = logging.getLogger(__name__)


def extract_task():
    """
    Extract task: Fetch all Pokémon and habitat data from PokéAPI.
    """
    logger.info("Starting extract task...")
    try:
        create_raw_folder()
        
        logger.info("Fetching Pokémon list...")
        fetch_pokemon_list()
        
        logger.info("Fetching Pokémon details...")
        fetch_pokemon_details()
        
        logger.info("Fetching habitat list...")
        fetch_habitat_list()
        
        logger.info("Fetching habitat details...")
        fetch_habitat_details()
        
        logger.info("Extract task completed successfully")
    except Exception as err:
        logger.error(f"Extract task failed: {err}")
        raise


def transform_task(**context):
    """
    Transform task: Process raw JSON data into structured DataFrames.
    Push DataFrames to XCom for the next task.
    """
    logger.info("Starting transform task...")
    try:
        logger.info("Transforming Pokémon data...")
        pokemon_df = create_pokemon_tables()
        
        logger.info("Transforming habitat data...")
        habitat_df = create_habitat_tables()
        
        # Push DataFrames to XCom as JSON
        logger.info("Pushing transformed data to XCom...")
        context["ti"].xcom_push(key="pokemon", value=pokemon_df.to_json())
        context["ti"].xcom_push(key="habitat", value=habitat_df.to_json())
        
        logger.info(f"Transform task completed: {len(pokemon_df)} Pokémon, {len(habitat_df)} habitat relationships")
    except Exception as err:
        logger.error(f"Transform task failed: {err}")
        raise


def load_task(**context):
    """
    Load task: Load transformed data into PostgreSQL database.
    Pull DataFrames from XCom and insert into database.
    """
    logger.info("Starting load task...")
    try:
        # Pull DataFrames from XCom
        logger.info("Retrieving transformed data from XCom...")
        pokemon_json = context["ti"].xcom_pull(key="pokemon")
        habitat_json = context["ti"].xcom_pull(key="habitat")
        
        if not pokemon_json or not habitat_json:
            raise ValueError("Failed to retrieve data from XCom")
        
        pokemon_df = pd.read_json(pokemon_json)
        habitat_df = pd.read_json(habitat_json)

        boolean_cols = ['ability_1_is_hidden', 'ability_2_is_hidden', 'ability_3_is_hidden']
        for col in boolean_cols:
            if col in pokemon_df.columns:
                pokemon_df[col] = pokemon_df[col].map({1.0: True, 0.0: False}, na_action='ignore')
        
        logger.info(f"Retrieved {len(pokemon_df)} Pokémon and {len(habitat_df)} habitat relationships")
        
        # Get database engine
        engine = get_engine()
        
        # Test connection
        if not test_connection(engine):
            raise ConnectionError("Failed to connect to database")
        
        logger.info("Dropping existing tables if they exist...")
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS habitat CASCADE"))
            conn.execute(text("DROP TABLE IF EXISTS pokemon CASCADE"))
        logger.info("Existing tables dropped")
        
        # Run schema file to create/update tables
        logger.info("Creating/updating database schema...")
        run_sql_file(engine, "schema.sql")
        
        # Load Pokémon data
        logger.info("Loading Pokémon data...")
        load_dataframe(engine, pokemon_df, "pokemon", if_exists='append')
        pokemon_count = verify_table_data(engine, "pokemon")
        
        # Load habitat data
        logger.info("Loading habitat data...")
        load_dataframe(engine, habitat_df, "habitat", if_exists='append')
        habitat_count = verify_table_data(engine, "habitat")
        
        logger.info(f"Load task completed: {pokemon_count} Pokémon, {habitat_count} habitat relationships in database")
        
    except Exception as err:
        logger.error(f"Load task failed: {err}")
        raise


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

# Create the DAG
with DAG(
    dag_id="pokemon_etl",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval='0 0 1 */6 *',  # Run at midnight on the 1st day of every 6th month (Jan 1 & Jul 1)
    catchup=False,
    tags=['pokemon', 'etl', 'scheduled'],
    description='Update Pokémon data every 6 months from PokéAPI'
) as dag:

    # Define tasks
    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_task,
        doc_md="""
        ### Extract Task
        Fetches Pokémon and habitat data from PokéAPI:
        - Pokémon list and details
        - Habitat list and details
        
        Saves raw JSON files to the configured RAW_DIR.
        """
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_task,
        doc_md="""
        ### Transform Task
        Processes raw JSON data into structured DataFrames:
        - Extracts abilities, types, and stats
        - Creates relational habitat data
        
        Pushes DataFrames to XCom for the load task.
        """
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_task,
        doc_md="""
        ### Load Task
        Loads transformed data into PostgreSQL database:
        - Creates/updates schema
        - Inserts Pokémon data
        - Inserts habitat relationships
        
        Verifies row counts after insertion.
        """
    )

    # Define task dependencies
    extract >> transform >> load