import findspark
findspark.init("/opt/cloudera/parcels/CDH-6.2.0-1.cdh6.2.0.p0.967373/lib/spark")
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, from_json, col, schema_of_json, to_timestamp
from pyspark.sql.types import StringType
from pyspark.sql.functions import radians, sin, cos, sqrt, atan2, pow as F_pow, lit
from pyspark.sql.functions import count
# Create a SparkSession
spark = SparkSession.builder.appName("Agrupar y contar aviones por categoría").getOrCreate()

# JSON example to define the structure
ejemplo_json = '{"ac": [{"flight": "RYR80XN ", "lat": 40.783493, "lon": -9.551697, "alt_baro": 37000, "category": "A3"}], "ctime": 1702444273059, "msg": "No error", "now": 1702444272731, "ptime": 6, "total": 146}'
esquema = spark.read.json(spark.sparkContext.parallelize([ejemplo_json])).schema

# Create the data stream connection to the port where the JSONs are emitted
flujo = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 10007) \
    .load()

# Parse the JSON text string
datos_json = flujo.select(from_json(col("value").cast("string"), esquema).alias("datos"))

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
    (col("timestamp") / 1000).alias("timestamp")
)

def calculate_distance(df, lat_origin, lon_origin, lat_dest, lon_dest, column_name):
    # Convert the airport coordinates to lit() so they can be used in column operations
    lat_dest = lit(lat_dest)
    lon_dest = lit(lon_dest)
    
    # Applies the Haversine formula
    a = ( 
        F_pow(sin(radians(lat_dest - col(lat_origin)) / 2), 2) +
        cos(radians(lat_dest)) * cos(radians(col(lat_origin))) * 
        F_pow(sin(radians(lon_dest - col(lon_origin)) / 2), 2)
    )
    distance = atan2(sqrt(a), sqrt(-a + 1)) * 12742e3  # Radius of the Earth in meters
    
    return df.withColumn(column_name, distance)

# Coordinates of the airports
barcelona_airport = (41.2971, 2.0785)
tarragona_airport = (41.1474, 1.1672)
girona_airport = (41.9010, 2.7606)

# Apply the distance calculation function for each airport
vuelos_df = calculate_distance(vuelos_df, "lat", "lon", barcelona_airport[0], barcelona_airport[1], "distancia_barcelona")
vuelos_df = calculate_distance(vuelos_df, "lat", "lon", tarragona_airport[0], tarragona_airport[1], "distancia_tarragona")
vuelos_df = calculate_distance(vuelos_df, "lat", "lon", girona_airport[0], girona_airport[1], "distancia_girona")
# Group the data by the 'category' column and count the occurrences
aviones_agrupados_df = vuelos_df.groupBy("category").count().orderBy(col("count").desc())

# Write the resulting Dataframe to the console for display
query = aviones_agrupados_df \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()