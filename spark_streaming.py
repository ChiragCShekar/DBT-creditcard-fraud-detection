'''from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StringType, FloatType, TimestampType

spark = SparkSession.builder.appName("CardDuplicationDetection").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType() \
    .add("card_id", StringType()) \
    .add("location", StringType()) \
    .add("amount", FloatType()) \
    .add("timestamp", StringType())

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "card-transactions") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Optional: write to MySQL here
def write_to_mysql(batch_df, batch_id):
    import mysql.connector
    conn = mysql.connector.connect(user='root', password='mansi@711', database='creditdb')
    cursor = conn.cursor()
    for row in batch_df.collect():
        cursor.execute("INSERT INTO transactions (card_id, location, amount, timestamp) VALUES (%s, %s, %s, %s)",
                       (row['card_id'], row['location'], row['amount'], row['timestamp']))
    conn.commit()
    cursor.close()
    conn.close()

query = json_df.writeStream \
    .foreachBatch(write_to_mysql) \
    .start()

query.awaitTermination()
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, FloatType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("CardDuplicationDetection") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema for incoming Kafka JSON data
schema = StructType() \
    .add("card_id", StringType()) \
    .add("location", StringType()) \
    .add("amount", FloatType()) \
    .add("timestamp", StringType())

# Read from Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "card-transactions") \
    .load()

# Parse JSON from Kafka 'value' column
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Write each micro-batch to MySQL
def write_to_mysql(batch_df, batch_id):
    print(f"\n🚀 Processing batch {batch_id}...")

    if batch_df.count() == 0:
        print("⚠️  Empty batch received. Skipping write.")
        return

    try:
        import mysql.connector
        conn = mysql.connector.connect(
            user='root',
            password='mansi@711',
            database='creditdb',
            host='localhost'
        )
        cursor = conn.cursor()

        for row in batch_df.collect():
            print(f"✅ Inserting: {row}")
            cursor.execute("""
                INSERT INTO transactions (card_id, location, amount, timestamp)
                VALUES (%s, %s, %s, %s)
            """, (row['card_id'], row['location'], row['amount'], row['timestamp']))

        conn.commit()
        cursor.close()
        conn.close()
        print("✅ Batch written to MySQL successfully.")
    except Exception as e:
        print(f"❌ Error writing batch to MySQL: {e}")

# Start the streaming job
query = json_df.writeStream \
    .foreachBatch(write_to_mysql) \
    .outputMode("update") \
    .start()

query.awaitTermination()
