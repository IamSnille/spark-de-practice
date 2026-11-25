Weather Data Pipeline

A small PySpark project: it downloads historical weather for Kyiv, Lviv, Warsaw and Krakow from the Open-Meteo API, reads the raw JSON with Spark using an explicit schema, flattens and transforms it, and saves Parquet partitioned by date. The ETL functions are covered by unit tests.

Data Flow

Open-Meteo API -> raw JSON (data/raw/weather) -> Spark (read, clean, transform) -> Parquet partitioned by date (data/output/result)

Project Structure
spark-de-practice/
├── src/
│   ├── ingest_api.py       # downloads raw JSON from the API
│   ├── functions.py        # ETL functions: read_data, clean, transform, write_data
│   └── main.py             # runs the pipeline using functions.py
├── test/
│   └── test_functions.py   # unit tests for functions.py
├── data/
│   ├── raw/weather/        # raw JSON files (generated)
│   └── output/             # Parquet files (generated)
├── pytest.ini
├── requirements.txt
└── README.md

data/ and venv/ are created locally and are not stored in Git.

Requirements
Python 3.10+
Java 17+
Git
Windows only: Hadoop winutils in C:\hadoop\bin (the code sets HADOOP_HOME=C:\hadoop)
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
python src/main.py               # 2. Spark: JSON -> Parquet in data/output/result/

The scripts write progress messages to the console using logging.

Linux / macOS

The project is set up for Windows. To run it on Linux or macOS, change one thing:

In src/main.py, the Python path points to the Windows virtual environment (venv\Scripts\python.exe). Replace it with venv/bin/python, or delete the three lines under # Python environment (functions.py already sets PYSPARK_PYTHON to the current Python).

HADOOP_HOME=C:\hadoop in src/functions.py is only needed on Windows and is ignored on other systems.

Run Tests
bash
pytest

The tests (test/test_functions.py) check every function in src/functions.py: format validation, reading, cleaning and flattening, the average temperature calculation and writing partitioned Parquet. pytest.ini tells pytest where to find the src folder.

Output

One row per city per day:

Column	Description
city	City name
latitude, longitude	Coordinates
date	Date (partition column, stored as date=YYYY-MM-DD folders)
temperature_max, temperature_min	Daily max / min temperature
precipitation	Daily precipitation (mm)
temperature_avg	Average of max and min temperature
What I Learned
Defining an explicit schema with StructType instead of letting Spark guess it.
Flattening nested arrays with arrays_zip() and explode().
Writing Parquet partitioned by date and checking partition pruning with explain().
Splitting a pipeline into small functions (read, clean, transform, write).
Writing unit tests for Spark functions with pytest.
Using logging instead of print.