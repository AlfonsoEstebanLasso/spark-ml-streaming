import findspark
findspark.init("/opt/cloudera/parcels/CDH-6.2.0-1.cdh6.2.0.p0.967373/lib/spark")
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, from_json, col, schema_of_json, to_timestamp
from pyspark.sql.types import StringType
from pyspark.sql.functions import radians, sin, cos, sqrt, atan2, pow as F_pow, lit
from pyspark.sql.functions import count
# Crear una SparkSession
spark = SparkSession.builder.appName("Agrupar y contar aviones por categoría").getOrCreate()

# Ejemplo de JSON para definir la estructura
ejemplo_json = '{"ac": [{"flight": "RYR80XN ", "lat": 40.783493, "lon": -9.551697, "alt_baro": 37000, "category": "A3"}], "ctime": 1702444273059, "msg": "No error", "now": 1702444272731, "ptime": 6, "total": 146}'
esquema = spark.read.json(spark.sparkContext.parallelize([ejemplo_json])).schema

# Crear la conexión de flujo de datos al puerto donde se emiten los JSON
flujo = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 10007) \
    .load()

# Interpretar la cadena de texto JSON
datos_json = flujo.select(from_json(col("value").cast("string"), esquema).alias("datos"))

# Explotar la lista de aviones para poner cada vuelo en una fila diferente
aviones_df = datos_json.select(explode(col("datos.ac")).alias("aviones"), col("datos.now").alias("timestamp"))

# Limitar los vuelos a los que están en el área definida (Cataluña)
vuelos_cataluna_df = aviones_df.filter(
    (col("aviones.lat") >= 40.294028) & (col("aviones.lat") <= 42.924299) &
    (col("aviones.lon") >= 0.500251) & (col("aviones.lon") <= 3.567923)
)

# Crear un dataframe con las columnas necesarias y convertir el campo "now" a un timestamp
vuelos_df = vuelos_cataluna_df.select(
    col("aviones.flight").alias("flight"),
    col("aviones.lat").alias("lat"),
    col("aviones.lon").alias("lon"),
    col("aviones.alt_baro").alias("alt_baro"),
    col("aviones.category").alias("category"),
    (col("timestamp") / 1000).alias("timestamp")
)

def calculate_distance(df, lat_origin, lon_origin, lat_dest, lon_dest, column_name):
    # Convertir las coordenadas del aeropuerto en lit() para poder utilizarlas en las operaciones de columna
    lat_dest = lit(lat_dest)
    lon_dest = lit(lon_dest)
    
    # Aplica la fórmula de Haversine
    a = ( 
        F_pow(sin(radians(lat_dest - col(lat_origin)) / 2), 2) +
        cos(radians(lat_dest)) * cos(radians(col(lat_origin))) * 
        F_pow(sin(radians(lon_dest - col(lon_origin)) / 2), 2)
    )
    distance = atan2(sqrt(a), sqrt(-a + 1)) * 12742e3  # Radio de la Tierra en metros
    
    return df.withColumn(column_name, distance)

# Coordenadas de los aeropuertos
barcelona_airport = (41.2971, 2.0785)
tarragona_airport = (41.1474, 1.1672)
girona_airport = (41.9010, 2.7606)

# Aplicar la función de cálculo de distancia para cada aeropuerto
vuelos_df = calculate_distance(vuelos_df, "lat", "lon", barcelona_airport[0], barcelona_airport[1], "distancia_barcelona")
vuelos_df = calculate_distance(vuelos_df, "lat", "lon", tarragona_airport[0], tarragona_airport[1], "distancia_tarragona")
vuelos_df = calculate_distance(vuelos_df, "lat", "lon", girona_airport[0], girona_airport[1], "distancia_girona")
# Agrupar los datos por la columna 'category' y contar las ocurrencias
aviones_agrupados_df = vuelos_df.groupBy("category").count().orderBy(col("count").desc())

# Escribir el Dataframe resultante en la consola para su visualización
query = aviones_agrupados_df \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()