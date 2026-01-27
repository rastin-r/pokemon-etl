# Pokémon ETL Pipeline

A data pipeline that collects Pokémon information from the PokéAPI, organizes it, and stores it in a database. Built as a learning project to practice ETL (Extract, Transform, Load) processes with Python and Apache Airflow.

## What This Project Does

- Fetches data about 1,350+ Pokémon from [PokéAPI](https://pokeapi.co)
- Cleans and organizes the data into easy-to-use tables
- Saves the data as CSV files and in a PostgreSQL database
- Automatically updates the data every 6 months using Airflow

## Technologies Used

- **Python** - Main programming language
- **Apache Airflow** - Schedules and runs the data pipeline
- **PostgreSQL** - Stores the data in a database
- **Docker** - Makes it easy to run everything without complex setup
- **pandas** - Helps with data processing

## Project Structure
```
pokemon-etl/
├── dags/
│   └── pokemon_etl_dags.py
├── docs/
│   └── images/
│       └── tables.drawio.svg
├── etl/
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   └── load_postgres.py
├── sql/
│   └── schema.sql
├── .env.example
├── .gitignore
├── docker-compose.yml
├── etl.py
├── README.md
└── requirements.txt
```

## Setup and Installation

### Option 1: Run Locally (Simple Way)

**1. Install Python 3.8 or higher**

**2. Clone this repository**
```bash
git clone https://github.com/rastin-r/pokemon-etl.git
cd pokemon-etl
```

**3. Set up environment variables**

Copy the environment template:
```bash
# Windows (Command Prompt)
copy .env.example .env

# Windows (PowerShell)
Copy-Item .env.example .env

# Mac/Linux
cp .env.example .env
```

**Note:** The default values work out of the box for local development. No changes needed!

**4. Create a virtual environment**
```bash
python -m venv venv

# Activate it:
# Windows (Command Prompt):
venv\Scripts\activate

# Windows (PowerShell):
venv\Scripts\Activate.ps1

# Mac/Linux:
source venv/bin/activate
```

**5. Install required packages**
```bash
pip install -r requirements.txt
```

**6. Run the pipeline**
```bash
python etl.py
```

**7. Check your results**
- CSV files will be in `output/csv/`
- SQL files will be in `output/sql/`
- Raw data will be in `output/raw/`

### Option 2: Run with Docker (For Airflow Scheduling)

**1. Make sure Docker is installed and running**

**2. Clone this repository**
```bash
git clone https://github.com/rastin-r/pokemon-etl.git
cd pokemon-etl
```

**3. Set up environment variables**

Copy the environment template:
```bash
# Windows (Command Prompt)
copy .env.example .env

# Windows (PowerShell)
Copy-Item .env.example .env

# Mac/Linux
cp .env.example .env
```

**Note:** The default values work for Docker. No changes needed!

**4. Start the services**
```bash
docker compose up -d
```

**5. Open Airflow in your browser**
- Go to http://localhost:8080
- Login with username: `admin`, password: `admin`

**6. Run the pipeline**
- Find `pokemon_etl` in the list
- Turn it ON using the toggle switch
- Click the play button to run it manually

**7. Stop the services when done**
```bash
docker compose down
```

## What Data Gets Collected

### Pokémon Table
Information about each Pokémon including:
- Name, ID and Species
- Types (like Fire, Water, Grass)
- Base stats (HP, Attack, Defense, etc.)
- Abilities

### Habitat Table
Shows where each Pokémon lives:
- Habitat name (cave, forest, sea, etc.)
- Which Pokémon species is found there

## Tables in Database

The two tables are connected:
- Each Pokémon species has one primary habitat
- Each habitat can have many Pokémon species

![Displays the tables Pokémon and Habitat](/docs/images/tables.drawio.svg)

## How to Use the Data

Once the pipeline runs, you can:

**Query the database:**
```bash
docker exec -it pokemon-postgres psql -U pokemon -d pokemon
```
```sql
-- See how many Pokémon you have
SELECT COUNT(*) FROM pokemon;

-- Find all Fire-type Pokémon
SELECT name, type_1, attack FROM pokemon WHERE type_1 = 'fire';

-- See Pokémon by habitat
SELECT h.habitat_name, COUNT(*) 
FROM habitat h 
GROUP BY h.habitat_name;
```

**Or use the CSV files:**
- Open `output/csv/pokemon.csv` in Excel or any spreadsheet program
- Use for data analysis, visualization, or your own projects

## Troubleshooting

**Can't access Airflow at localhost:8080?**
- Make sure Docker is running
- Wait a full minute after running `docker compose up -d`
- Try http://127.0.0.1:8080 instead

**Pipeline not showing up in Airflow?**
- Check the scheduler logs: `docker compose logs airflow-scheduler`
- Restart the scheduler: `docker compose restart airflow-scheduler`

**Port already in use?**
- Change the port in `docker-compose.yml` from `8080:8080` to something like `8081:8080`

## What I Learned

- How to work with REST APIs to get data
- Processing JSON data with Python
- Using pandas for data transformation
- Scheduling tasks with Apache Airflow
- Using Docker for consistent development environments

## Future Improvements

- Add data validation and quality checks
- Create visualizations of the data
- Add more Pokémon attributes (moves, evolution chains, etc.)

## Disclaimer

AI was used for writing the README.md and cleaning up the code but the logic was implemented by me :)

