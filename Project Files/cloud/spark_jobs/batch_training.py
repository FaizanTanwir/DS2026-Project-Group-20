from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
import shutil
import time
import os

# ===============================
# PATH CONFIGURATION
# ===============================
SPARK_MODEL_PATH = "/ml_models/spark_rf_model"
VERSION_FILE = "/ml_models/version.txt"

os.makedirs("/ml_models", exist_ok=True)

# ===============================
# CREATE SPARK SESSION (Distributed Cluster)
# ===============================
spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("IoTBatchTraining") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Connected to Spark cluster.")
# ===============================
# LOAD DATA FROM POSTGRESQL
# ===============================
df = spark.read \
    .format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", "jdbc:postgresql://postgres:5432/iotdb") \
    .option("dbtable", "sensor_data") \
    .option("user", "iotuser") \
    .option("password", "iotpass") \
    .load()

print("Total rows loaded:", df.count())

# ===============================
# BASIC AGGREGATION (DISTRIBUTED)
# ===============================
df.groupBy("gateway_id").count().show()

# ===============================
# PREPARE LABEL COLUMN
# ===============================
df = df.withColumn(
    "label",
    when(col("classification") == "ANOMALY", 1).otherwise(0)
)

# ===============================
# FEATURE VECTOR ASSEMBLY
# ===============================
assembler = VectorAssembler(
    inputCols=["temperature", "humidity"],
    outputCol="features"
)

# ===============================
# RANDOM FOREST (DISTRIBUTED)
# ===============================
rf = RandomForestClassifier(
    featuresCol="features",
    labelCol="label",
    numTrees=50,
    maxDepth=5
)

# ===============================
# PIPELINE
# ===============================
pipeline = Pipeline(stages=[assembler, rf])

# ===============================
# TRAIN MODEL (DISTRIBUTED)
# ===============================
model = pipeline.fit(df)

print("Model training completed successfully.")

# ===============================
# SAVE SPARK MODEL 
# (Gateway/app.py Loads This)
# ===============================
# Remove old shared model if exists
if os.path.exists(SPARK_MODEL_PATH):
    shutil.rmtree(SPARK_MODEL_PATH)

# Save DIRECTLY to shared volume
model.write().overwrite().save(SPARK_MODEL_PATH)

print(f"Spark model saved to shared volume: {SPARK_MODEL_PATH}")

# Validate save
if not os.path.exists(SPARK_MODEL_PATH):
    raise Exception("Model save failed — directory not created!")

# ===============================
# UPDATE MODEL VERSION
# ===============================
version = 1
if os.path.exists(VERSION_FILE):
    with open(VERSION_FILE, "r") as f:
        version = int(f.read().strip()) + 1

with open(VERSION_FILE, "w") as f:
    f.write(str(version))

print(f"Model version updated to {version}")

# ===============================
# STOP SPARK
# ===============================
spark.stop()
print("Connected to Spark cluster.")