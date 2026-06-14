from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, count ,window , expr , date_format
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

# 1. Ορισμός του Schema
schema = (
    StructType()
    .add("name", StringType())
    .add("dn",   IntegerType())
    .add("orig", StringType())
    .add("dest", StringType())
    .add("t",    DoubleType())
    .add("link", StringType())
    .add("x",    DoubleType())
    .add("s",    DoubleType())
    .add("v",    DoubleType())
)

# 2. Δημιουργία Spark Session
spark = (
    SparkSession.builder
    .appName("UXSIM-Consumer")
    #.master("local[*]")# τοπικο spark
    .master("spark://spark-master:7077")
    .config("spark.mongodb.write.connection.uri", "mongodb://mongo:27017")
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.default.parallelism", "2") \
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# 3. Σύνδεση στον Redpanda
df = (
    spark.readStream
    .format("kafka")
    #.option("kafka.bootstrap.servers", "localhost:19092")
    .option("kafka.bootstrap.servers", "redpanda:9092")
    .option("subscribe", "vehicle_positions")
    .option("startingOffsets", "latest")
    .load()
)

# 4. Parsing JSON → Structured DataFrame
parsed = (
    df
    .selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
)

# 5. Επεξεργασία: Ομαδοποίηση ανά 10 δευτερόλεπτα και υπολογισμός μέσου όρου ταχύτητας και πλήθους οχημάτων ανά link
parsed = parsed.withColumn(
    "event_time",

    expr("timestamp_seconds(t)")

)
parsed = parsed.withWatermark("event_time", "10 seconds")

stats = (

    parsed
    .groupBy(

        window(col("event_time"), "10 seconds").alias("time"),
        col("link")

    )

    .agg(
        count("*").alias("vcount"),
        avg("v").alias("vspeed")

    )

    .select(
        date_format(col("time.start"), "HH:mm:ss").alias("time"),
        col("link"),
        col("vcount"),
        col("vspeed")

    )

)

# 6a. WRITE RAW DATA TO MONGO
def write_raw(batch_df, batch_id):

    if batch_df.count() == 0:

        return

    batch_df.write \
        .format("mongodb") \
        .mode("append") \
        .option("database", "traffic") \
        .option("collection", "raw_data") \
        .save()

    print(f"[RAW] Batch {batch_id} written ({batch_df.count()} rows)")

query_raw = (

    parsed.writeStream
    
    .outputMode("append")
    .foreachBatch(write_raw)
    .option("checkpointLocation", "/tmp/checkpoints/raw")
    .trigger(processingTime="10 seconds")
    .start()

)

# 6β. Αποθήκευση στη MongoDB των επεξεργασμενων δεδομενων
def write_stats(batch_df, batch_id):

    if batch_df.count() == 0:

        return

    df_clean = batch_df.select(
        col("time"),
        col("link"),
        col("vcount"),
        col("vspeed")

    )

    df_clean.write \
        .format("mongodb") \
        .mode("append") \
        .option("database", "traffic") \
        .option("collection", "stats") \
        .save()

    print(f"[STATS] Batch {batch_id} written ({df_clean.count()} rows)")

query_stats = (

    stats.writeStream

    .outputMode("append")
    .foreachBatch(write_stats)
    .option("checkpointLocation", "/tmp/checkpoints/stats")
    .trigger(processingTime="10 seconds")
    .start()

)

# 7. Προβολή στην κονσόλα για debugging

query_console = (
    stats.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .trigger(processingTime="30 seconds")
    .start()

)
# Αναμονή
spark.streams.awaitAnyTermination()