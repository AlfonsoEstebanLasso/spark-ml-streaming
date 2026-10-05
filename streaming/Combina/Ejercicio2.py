import findspark
findspark.init("/opt/cloudera/parcels/CDH-6.2.0-1.cdh6.2.0.p0.967373/lib/spark")
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, unix_timestamp, col

spark = SparkSession.builder.appName("SparkStreaming").getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", "1")

# Impressions stream
impresiones = (
  spark
    .readStream.format("rate").option("rowsPerSecond", "1").option("numPartitions", "1").load()
    .selectExpr("value AS idAnuncio", "timestamp AS tiempoImpresion")
)

# Clicks stream
clicks = (
  spark
    .readStream.format("rate").option("rowsPerSecond", "1").option("numPartitions", "1").load()
    .selectExpr("value AS idAnuncio", "timestamp AS tiempoClick")
    .where(rand() < 0.2)  # We keep 20% of the rows
)

# Combine the streams
combined = impresiones.join(
  clicks,
  on="idAnuncio",
  how="inner"
).withWatermark("tiempoImpresion", "15 seconds").withWatermark("tiempoClick", "15 seconds")

# Add a column with the time difference
combined = combined.withColumn(
    "deltaT", 
    (unix_timestamp(col("tiempoClick")) - unix_timestamp(col("tiempoImpresion")))
)

# Show the combined stream
resCombined = (
  combined
    .writeStream
    .outputMode("append")
    .format("console")
    .trigger(processingTime='5 seconds') 
    .start()
)

resCombined.awaitTermination()