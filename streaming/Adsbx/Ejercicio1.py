import findspark
findspark.init("/opt/cloudera/parcels/CDH-6.2.0-1.cdh6.2.0.p0.967373/lib/spark")
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, from_json, schema_of_json, col, struct, to_json
from pyspark.sql.types import StringType

ejemplo='{"ac": [{"flight": "RYR80XN ","lat": 40.783493,"lon": -9.551697, "alt_baro": 37000,"category": "A3"}], "ctime": 1702444273059, "msg": "No error", "now": 1702444272731, "ptime": 6, "total": 146}'
spark = SparkSession.builder.appName("STRUCTURED STREAMING").getOrCreate()

# Infer the schema of the JSON
esquema = schema_of_json(ejemplo)

flujo = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 10007) \
    .load()

datos = flujo.select(from_json(col("value").cast("string"), esquema).alias("parsed_value"))

# We use struct to group the keys into a single struct and then convert that struct into a JSON string
datos_seleccionados = datos.select(
    explode(col("parsed_value.ac")).alias("ac"),
    to_json(struct(
        col("parsed_value.ctime").alias("ctime"),
        col("parsed_value.msg").alias("msg"),
        col("parsed_value.now").alias("now"),
        col("parsed_value.ptime").alias("ptime"),
        col("parsed_value.total").alias("total")
    )).alias("value")
)

resultado_final = datos_seleccionados.select(
    "ac.flight",
    "ac.lat",
    "ac.lon",
    "ac.alt_baro",
    "ac.category",
    "value"
)

resultado = resultado_final \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

resultado.awaitTermination()