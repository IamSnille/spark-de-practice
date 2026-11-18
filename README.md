Weather Data Pipeline

A small PySpark project: it downloads historical weather for Kyiv, Lviv, Warsaw and Krakow from the Open-Meteo API, reads the raw JSON with Spark using an explicit schema, flattens it and saves Parquet partitioned by date.

Data Flow

Open-Meteo API -> raw JSON (data/raw/weather) -> Spark -> Parquet partitioned by date (data/output/weather)

Project Structure
spark-de-practice/
├── src/
│   ├── ingest_api.py       # downloads raw JSON from the API
│   ├── raw_to_parquet.py   # raw JSON -> flat DataFrame -> Parquet
│   ├── functions.py        # ETL functions: read_data, clean, transform, write_data
│   └── main.py             # runs the functions from functions.py
├── data/
│   ├── raw/weather/        # raw JSON files (generated)
│   └── output/             # Parquet files (generated)
├── tests/
├── requirements.txt
└── README.md

data/ and venv/ are created locally and are not stored in Git.

Requirements

Python 3.10+, Java 17+, Git.

Setup
bash
git clone https://github.com/IamSnille/spark-de-practice.git
cd spark-de-practice
python -m venv venv
venv\Scripts\Activate.ps1        # Linux / macOS: source venv/bin/activate
pip install -r requirements.txt
Run

Run from the project root:

bash
python src/ingest_api.py         # 1. download JSON to data/raw/weather/
python src/main.py    # 2. Spark: JSON -> Parquet in data/output/weather/
Output

One row per city per day:

Column	Description
city	City name
latitude, longitude	Coordinates
date	Date (partition column)
temp_max, temp_min	Daily max / min temperature
precipitation_mm	Daily precipitation
What I Learned
Defining an explicit schema with StructType instead of letting Spark guess it.
Flattening nested arrays with arrays_zip() and explode().
Writing Parquet partitioned by date and checking partition pruning with explain().