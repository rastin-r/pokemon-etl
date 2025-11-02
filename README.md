# Pokémon ETL Pipeline

This project is a simple ETL (Extract, Transform, Load) pipeline that retrieves data from the [PokéAPI](pokeapi.co), processes it into structured tables, and saves the results as CSV files.

It demonstrates how to perform real-world data engineering tasks such as data extraction from an API, data transformation using pandas, and data loading into CSV files — all with proper logging and error handling.

# Project Structure

.

├── extract.py - Extracts Pokémon data from the PokéAPI and saves raw JSON

├── transform.py - Transforms raw JSON data into structured pandas DataFrames

├── load.py - Loads DataFrames into CSV files

├── etl.py - Main pipeline orchestrator

├── requirements.txt - Python dependencies

├── raw/ - Raw data output directory (created automatically)

├── transformed/ - Intermediate transformed data (created automatically)

└── data/ - Final CSV output directory (created automatically)

# Installation

1. Clone the repository (or download the project folder):

   ``` bash
    git clone https://github.com/rastin-r/pokemon-etl.git

    cd pokemon-etl
    ```
2. Create a virtual environment (recommended):

    Type the following:

    ```bash
    python -m venv venv
    ```
    If you are on Mac or Linux then type this:

    ```bash
    source venv/bin/activate
    ```
    or if you are on Windows then type this instead:

    ```cmd
    venv\Scripts\activate
    ```
3. Install dependencies:

    ```bash
    pip install -r requirements.txt
    ```

# Running the ETL Pipeline

Run the main script:

```cmd
python etl.py
```
This will:

<b>Extract</b> — Fetch the list of all Pokémon and detailed data from the [PokéAPI](pokeapi.co), saving results in the `raw/` folder.

<b>Transform</b> — Parse and structure the raw JSON into clean pandas DataFrames.

<b>Load</b> — Save the processed data into CSV files located in the `data/` directory.

# Output files

After running the pipeline you will have the following files

| File                       | Description                                               |
| -------------------------- | --------------------------------------------------------- |
| raw/pokemon_list.json      | List of all Pokémon from the API                          |
| raw/pokemon_data.json      | Detailed Pokémon data                                     |
| data/pokemon.csv           | Basic Pokémon attributes (id, name, height, weight, etc.) |
| data/pokemon_types.csv     | Pokémon types                                             |
| data/pokemon_abilities.csv | Pokémon abilities and metadata                            |

# Requirements

All required dependencies are in requirements.txt

# License

This project is open source and free to use for educational and non-commercial purposes.

#

<b>Author</b>: Rastin Reza

<b>API</b>: [PokéAPI](pokeapi.co)