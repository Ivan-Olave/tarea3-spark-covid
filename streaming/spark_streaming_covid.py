from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

spark = SparkSession.builder \
    .appName("COVID19_Kafka_Spark") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Esquema de datos COVID
schema = StructType([
    StructField("departamento", StringType()),
    StructField("casos", IntegerType()),
    StructField("muertes", IntegerType()),
    StructField("recuperados", IntegerType()),
    StructField("timestamp", IntegerType())
])

# Leemos desde Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "covid19_colombia") \
    .load()

# Convertimos JSON
parsed = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# 🔥 Análisis en streaming
stats = parsed.groupBy("departamento").agg(
    {"casos": "avg", "muertes": "avg", "recuperados": "avg"}
)

# Salida en consola
query = stats.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
