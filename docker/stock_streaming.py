from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

spark = SparkSession.builder.appName("StockStream").getOrCreate()

schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("close", DoubleType(), True)
])

df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "broker:29092").option("subscribe", "stock-quotes").load()
parsed = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
query = parsed.writeStream.format("console").start()
query.awaitTermination()
