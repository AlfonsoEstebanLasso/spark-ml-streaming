import findspark
findspark.init("/opt/cloudera/parcels/CDH-6.2.0-1.cdh6.2.0.p0.967373/lib/spark")
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand

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

# Show the impressions stream
resImpresiones = (
  impresiones
    .writeStream
    .outputMode("append")
    .format("console")
    .trigger(processingTime='3 seconds') 
    .start()
)

# Show the clicks stream
resClicks = (
  clicks
    .writeStream
    .outputMode("append")
    .format("console")
    .trigger(processingTime='5 seconds') 
    .start()
)

resImpresiones.awaitTermination()
resClicks.awaitTermination()
