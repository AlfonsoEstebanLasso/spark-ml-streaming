import findspark
findspark.init("/opt/cloudera/parcels/CDH-6.2.0-1.cdh6.2.0.p0.967373/lib/spark")
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, from_json, col, schema_of_json, to_timestamp
from pyspark.sql.types import StringType

# Create a SparkSession
spark = SparkSession.builder.appName("Filtrado de vuelos y conversión de timestamp").getOrCreate()

# JSON example to define the structure
ejemplo_json = '{"ac": [{"flight": "RYR80XN ", "lat": 40.783493, "lon": -9.551697, "alt_baro": 37000, "category": "A3"}], "ctime": 1702444273059, "msg": "No error", "now": 1702444272731, "ptime": 6, "total": 146}'
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
aviones_df = datos_json.select(explode(col("datos.ac")).alias("aviones"), col("datos.now").alias("timestamp"))

# Limit the flights to those within the defined area (Catalonia)
vuelos_cataluna_df = aviones_df.filter(
    (col("aviones.lat") >= 40.294028) & (col("aviones.lat") <= 42.924299) &
    (col("aviones.lon") >= 0.500251) & (col("aviones.lon") <= 3.567923)
)

# Create a dataframe with the required columns and convert the "now" field to a timestamp
vuelos_df = vuelos_cataluna_df.select(
    col("aviones.flight").alias("flight"),
    col("aviones.lat").alias("lat"),
    col("aviones.lon").alias("lon"),
    col("aviones.alt_baro").alias("alt_baro"),
    col("aviones.category").alias("category"),
    (to_timestamp(col("timestamp") / 1000.0)).alias("timestamp")
)

# Write the resulting Dataframe to the console for display
query = vuelos_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()