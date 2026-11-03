Weather Data Pipeline

Project Description

This project collects historical weather data for Kyiv, Lviv, and Warsaw using the Open-Meteo API. The data is processed with Apache Spark and stored in Parquet format for further analysis.

Data Flow

Open-Meteo API
      |
      v
  Raw JSON files
      |
      v
  Apache Spark
      |
      v
  Flattened DataFrame
      |
      v
  Parquet files
  (partitioned by date)

Project Structure

spark-de-practice/
│
├── src/
│   ├── main.py              # Main Spark processing job
│   ├── ingest_api.py        # Downloads weather data from Open-Meteo API
│   ├── raw_to_parquet.py    # Raw JSON to Parquet processing
│   └── functions.py         # Helper functions
│
├── data/
│   ├── raw/
│   │   └── weather/         # Raw JSON files from the API
│   │
│   └── output/
│       └── weather/         # Processed Parquet data
│
├── tests/                   # Experiments and tests
│
├── requirements.txt         # Python dependencies
└── README.md                # Project documentation

How to Run

1. Install requirements

pip install -r requirements.txt

2. Run the ingestion script

python src/ingest_api.py

This downloads weather data from the Open-Meteo API and saves it as JSON files in data/raw/weather/.

3. Run the Spark job

python src/raw_to_parquet.py

This reads the raw weather JSON files, applies an explicit schema, flattens the daily weather arrays, and writes the result to Parquet files partitioned by date.

What I Learned

Explicit Schema: Defining a schema manually with StructType, StructField, and ArrayType instead of relying on automatic schema inference.

Flattening Nested Data: Using arrays_zip() and explode() to transform nested JSON arrays into individual daily records.

Parquet and Partitioning: Writing data in Parquet format and partitioning it by date to improve query performance through partition pruning.

