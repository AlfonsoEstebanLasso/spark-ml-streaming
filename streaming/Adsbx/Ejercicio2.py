import findspark
findspark.init("/opt/cloudera/parcels/CDH-6.2.0-1.cdh6.2.0.p0.967373/lib/spark")
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, from_json, col, schema_of_json
from pyspark.sql.types import StringType

# Create a SparkSession
spark = SparkSession.builder.appName("Pre-procesado de JSON").getOrCreate()

# JSON example to define the structure
ejemplo_json = '{"ac": [{"flight": "RYR80XN ","lat": 40.783493,"lon": -9.551697, "alt_baro": 37000,"category": "A3"}], "ctime": 1702444273059, "msg": "No error", "now": 1702444272731, "ptime": 6, "total": 146}'
esquema = schema_of_json(ejemplo_json)  # Detect the structure present in the JSON string

# Create the data stream connection to the port where the JSONs are emitted
flujo = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 10007) \
    .load()

# Parse the JSON text string
datos_json = flujo.select(from_json(col("value").cast(StringType()), esquema).alias("datos"))

# Explode the list of aircraft to put each flight in a different row
aviones_df = datos_json.select(explode(col("datos.ac")).alias("aviones"))

# Create a dataframe with the columns 'flight', 'lat', 'lon', 'alt_baro', 'category'
vuelos_df = aviones_df.select(
    col("aviones.flight").alias("flight"),
    col("aviones.lat").alias("lat"),
    col("aviones.lon").alias("lon"),
    col("aviones.alt_baro").alias("alt_baro"),
    col("aviones.category").alias("category")
)

# Write the resulting Dataframe to the console for display
query = vuelos_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
