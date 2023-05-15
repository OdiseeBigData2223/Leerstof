# Kafka consumer met pyspark om en charcount uit te voeren
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import length, col, size, split, window, count

spark = SparkSession.builder.appName("process tweets").getOrCreate()
spark.sparkContext.setLogLevel("ERROR") # reduce spam of logging

df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "BookStream").option("startingOffsets", "earliest").load()

df.printSchema()

lines = df.selectExpr("CAST(value as STRING)", "timestamp")
# count length of entire column
lines = lines.withColumn("length", length(col("value")))

cols = {"length":"count"}

# count letters
for a in "abcdefghijklmnopqrstuvwxyz0123456789":
    lines = lines.withColumn(str(a), size(split(col("value"), str(a)))-1)
    cols[str(a)] = "sum"
    
lines = lines.groupBy(
    window(lines.timestamp, "10 seconds", "1 second")
).agg(cols)


lines.writeStream.outputMode("complete").format("console").start().awaitTermination()
