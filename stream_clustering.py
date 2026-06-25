from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count, avg, round as sround
from pyspark.sql.types import StructType, StringType, DoubleType, LongType
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans

spark = SparkSession.builder.appName("RetailStreamClustering").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Fields we read from each JSON message (numeric features + country for context)
schema = (StructType()
          .add("quantity", LongType())
          .add("price", DoubleType())
          .add("revenue", DoubleType())
          .add("country", StringType()))

# Read the live Kafka stream via the internal listener (kafka:29092)
raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "kafka:29092")
       .option("subscribe", "retail-stream")
       .option("startingOffsets", "earliest")
       .load())

# Kafka value is bytes -> string -> parse JSON into typed columns
parsed = (raw.selectExpr("CAST(value AS STRING) AS json")
          .select(from_json(col("json"), schema).alias("d"))
          .select("d.*"))

# Cluster each micro-batch of transactions as it arrives
def cluster_batch(batch_df, batch_id):
    n = batch_df.count()
    if n == 0:
        return
    feats = batch_df.na.drop(subset=["quantity", "price", "revenue"])
    assembler = VectorAssembler(inputCols=["quantity", "price", "revenue"], outputCol="raw_features")
    scaler = StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True)
    vec = assembler.transform(feats)
    vec = scaler.fit(vec).transform(vec)            # standardise so no single feature dominates
    model = KMeans(k=3, seed=42, featuresCol="features").fit(vec)
    preds = model.transform(vec)
    print("\n===== Micro-batch " + str(batch_id) + ": " + str(n) + " transactions clustered =====")
    (preds.groupBy("prediction")
          .agg(count("*").alias("transactions"),
               sround(avg("quantity"), 1).alias("avg_qty"),
               sround(avg("price"), 2).alias("avg_price"),
               sround(avg("revenue"), 2).alias("avg_revenue"))
          .orderBy("prediction").show())

query = (parsed.writeStream
         .foreachBatch(cluster_batch)
         .trigger(processingTime="5 seconds")
         .start())

query.awaitTermination(timeout=60)    # run ~60s, then stop
query.stop()
spark.stop()
print("Streaming clustering finished.")
