import findspark
findspark.init("/opt/cloudera/parcels/CDH-6.2.0-1.cdh6.2.0.p0.967373/lib/spark")
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, unix_timestamp, col

spark = SparkSession.builder.appName("SparkStreaming").getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", "1")

# Stream de impresiones
impresiones = (
  spark
    .readStream.format("rate").option("rowsPerSecond", "1").option("numPartitions", "1").load()
    .selectExpr("value AS idAnuncio", "timestamp AS tiempoImpresion")
)

# Stream de clicks
clicks = (
  spark
    .readStream.format("rate").option("rowsPerSecond", "1").option("numPartitions", "1").load()
    .selectExpr("value AS idAnuncio", "timestamp AS tiempoClick")
    .where(rand() < 0.2)  # Nos quedamos con el 20% de las filas
)

# Combinar los streams
combined = impresiones.join(
  clicks,
  on="idAnuncio",
  how="inner"
).withWatermark("tiempoImpresion", "15 seconds").withWatermark("tiempoClick", "15 seconds")

# Añadir columna con la diferencia de tiempo
combined = combined.withColumn(
    "deltaT", 
    (unix_timestamp(col("tiempoClick")) - unix_timestamp(col("tiempoImpresion")))
)

# Mostrar el stream combinado
resCombined = (
  combined
    .writeStream
    .outputMode("append")
    .format("console")
    .trigger(processingTime='5 seconds') 
    .start()
)

resCombined.awaitTermination()