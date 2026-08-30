# PySpark Machine Learning and Structured Streaming

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=flat-square)](LICENSE)
[![Python 3](https://img.shields.io/badge/Python-3-3776AB?style=flat-square&logo=python&logoColor=white)](https://www.python.org/)
[![Apache Spark 2.4](https://img.shields.io/badge/Apache%20Spark-2.4-E25A1C?style=flat-square&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Spark MLlib](https://img.shields.io/badge/Spark%20MLlib-E25A1C?style=flat-square&logo=apachespark&logoColor=white)](https://spark.apache.org/mllib/)
[![Structured Streaming](https://img.shields.io/badge/Structured%20Streaming-E25A1C?style=flat-square&logo=apachespark&logoColor=white)](https://spark.apache.org/streaming/)
[![Cloudera CDH 6.2](https://img.shields.io/badge/Cloudera-CDH%206.2-F96702?style=flat-square&logo=cloudera&logoColor=white)](https://www.cloudera.com/)

**Predicting ICU admission from ~460k COVID-19 patient records with spark.ml, and streaming live ADS-B aircraft positions with Spark Structured Streaming — all on a Cloudera cluster ✈️**

Coursework project — BSc in Applied Data Science, Universitat Oberta de Catalunya (UOC), Data Analysis in Big Data Environments course.

Two pieces of PySpark work developed on a Cloudera CDH 6.2 cluster:

- **`pyspark_ml_covid.ipynb`** — a machine-learning pipeline on Mexico's open COVID-19 patient dataset: filtering and cleaning, descriptive and visual analysis, logistic regression with class-imbalance handling, and a decision tree with a manual interpretation of the fitted tree.
- **`streaming/`** — Spark Structured Streaming scripts: parsing a live JSON feed of aircraft positions from a socket, geographic filtering, distance calculations, and stream–stream joins on rate sources, each with a console screenshot of the running job.

> Notebook narrative and code comments are in Spanish (original coursework code, kept as written). All new text and filenames in this repository are in English.

## Objective

- Build an end-to-end spark.ml classification pipeline on a real public-health dataset (~460k records): predict ICU admission (`UCI`) from clinical and demographic variables, detect and correct class imbalance, and compare logistic regression against a decision tree.
- Practise Spark Structured Streaming: ingest and parse a nested JSON stream, transform it with column expressions, and combine multiple streams with watermarks.

## Data & methods

**COVID-19 dataset (not included).** Open data on COVID-19 patients published by Mexico's Ministry of Health (Secretaría de Salud): [datos abiertos](https://www.gob.mx/salud/documentos/datos-abiertos-152127) (a historical mirror with a variable catalogue is kept at [coronamex/datos](https://github.com/coronamex/datos)). One row per tested patient with sex, age, date of death, comorbidities (diabetes, COPD, asthma, immunosuppression, hypertension, cardiovascular disease, obesity, chronic kidney disease, smoking), lab result and ICU admission. Clinical variables are coded 1 = yes / 2 = no, with 97/98/99 as missing-value codes.

Notebook pipeline: keep lab-confirmed positives → select variables → drop rows with null or missing-coded values (recoding the target `UCI` so "not applicable" counts as "not admitted") → descriptive statistics and matplotlib charts (ICU admissions by age, deaths by age, comorbidity prevalence) → `VectorAssembler` + 80/20 split → `LogisticRegression` → diagnose majority-class collapse via the label distribution → oversample the minority class with replacement and retrain → `DecisionTreeClassifier` with `toDebugString` interpretation → accuracy and confusion matrices via `MulticlassMetrics`.

**Aircraft positions (`streaming/Adsbx/`).** Live ADS-B aircraft state vectors from the [AircraftScatter API on RapidAPI](https://rapidapi.com/adsbx/api/aircraftscatter), relayed to a local socket by a course-provided feeder script (not included). The scripts read the socket with `readStream.format("socket")`, infer the JSON schema from a sample document (`schema_of_json` / `spark.read.json`), flatten the aircraft array with `from_json` + `explode`, filter flights to a Catalonia bounding box, convert epoch-millisecond timestamps, compute haversine distances to the Barcelona, Reus (Tarragona) and Girona airports as pure Spark column expressions, and aggregate counts per aircraft category in `complete` output mode.

**Rate-source streams (`streaming/Combina/`).** Two synthetic streams built with Spark's `rate` source (ad impressions, and a ~20% random subset acting as clicks), joined stream-to-stream with 15-second watermarks and a click-delay (`deltaT`) column. No external data needed.

The `.PNG` files next to each script are screenshots of the micro-batch console output, showing each job running on the cluster.

## Key results

- 457,912 lab-confirmed positive patients loaded; 454,703 records after cleaning missing-value codes.
- Strong class imbalance: 11,278 ICU admissions vs 443,425 non-admissions (~2.5%). The first logistic regression predicted "not admitted" for all 90,403 test records.
- After oversampling the minority class: logistic regression accuracy 0.677 and decision tree accuracy 0.680, with roughly symmetric confusion matrices (metrics computed on the balanced test set — oversampling was applied before the split, so test data is resampled).
- The fitted decision tree (depth 5, 35 nodes) splits primarily on age, then diabetes and sex; the notebook includes a node-by-node reading of `toDebugString`.

## Tech stack

- PySpark (Spark 2.4 on a Cloudera CDH 6.2 cluster) — DataFrame API, spark.ml (`VectorAssembler`, `LogisticRegression`, `DecisionTreeClassifier`), spark.mllib (`MulticlassMetrics`), Structured Streaming (socket and rate sources, watermarks, stream–stream joins)
- findspark, pandas, matplotlib, Jupyter

## How to run

The code was written for Spark 2.4 on a Cloudera cluster but only uses standard APIs.

**Notebook.** In an environment with Spark and the packages in `requirements.txt`: download the COVID open dataset, adjust the CSV path in the load cell, and run `pyspark_ml_covid.ipynb` in Jupyter. The first cell creates a local `SparkContext` via findspark.

**Streaming scripts.** Each script is standalone (`python Ejercicio1.py`, etc.). Edit the `findspark.init(...)` path to point at your Spark installation. The `Adsbx/` scripts expect a feeder process writing AircraftScatter JSON documents (one per line) to `localhost:10007`; the `Combina/` scripts use the built-in rate source and need no external feed. Jobs print micro-batches to the console until interrupted.

## Repository structure

```
spark-ml-streaming/
├── pyspark_ml_covid.ipynb      # ML pipeline on COVID-19 open data (ICU prediction)
├── streaming/
│   ├── Adsbx/                  # Structured Streaming on live aircraft positions
│   │   ├── Ejercicio1.py       # Parse JSON stream, keep raw metadata as JSON column
│   │   ├── Ejercicio2.py       # Flatten aircraft array into one row per flight
│   │   ├── Ejercicio3.py       # Catalonia bounding-box filter + timestamp conversion
│   │   ├── Ejercicio4_1.py     # Haversine distances to BCN / Reus / Girona airports
│   │   ├── Ejercicio4_2.py     # Aggregation: aircraft count per category
│   │   └── Ejercicio*.PNG      # Console output screenshots of each job
│   └── Combina/                # Stream-stream joins on rate sources
│       ├── Ejercicio1.py       # Impressions + clicks streams with separate triggers
│       ├── Ejercicio2.py       # Watermarked inner join with click-delay column
│       └── Ejercicio*.PNG      # Console output screenshots
├── requirements.txt
└── README.md
```

Not included: the COVID CSV and any other datasets (see links above), the course-provided socket feeder for the AircraftScatter API, and a further course exercise on Kafka consuming the Norwegian public AIS vessel stream (mostly course-provided template, so not published here).
