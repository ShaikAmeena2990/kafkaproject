import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    print("Keyspace created successfully!")


def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.cisco_prices (
            id UUID PRIMARY KEY,
            timestamp TEXT,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            adjclose DOUBLE,
            volume BIGINT
        );
    """)
    print("Table created successfully!")


def create_spark_connection():
    try:
        spark = SparkSession.builder \
            .appName("CiscoPriceStreaming") \
            .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
            .config("spark.cassandra.connection.host", "localhost") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")
        logging.info("Spark session created.")
        return spark
    except Exception as e:
        logging.error(f"Failed to create Spark session: {e}")
        return None


def connect_to_kafka(spark):
    try:
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "cisco_prices") \
            .option("startingOffsets", "earliest") \
            .load()
        logging.info("Connected to Kafka topic.")
        return df
    except Exception as e:
        logging.error(f"Failed to read from Kafka: {e}")
        return None


def create_cassandra_connection():
    try:
        cluster = Cluster(["localhost"])
        session = cluster.connect()
        return session
    except Exception as e:
        logging.error(f"Could not connect to Cassandra: {e}")
        return None


def process_stream_data(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("open", DoubleType(), False),
        StructField("high", DoubleType(), False),
        StructField("low", DoubleType(), False),
        StructField("close", DoubleType(), False),
        StructField("adjclose", DoubleType(), False),
        StructField("volume", LongType(), False),
    ])

    parsed_df = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    return parsed_df


if __name__ == "__main__":
    spark = create_spark_connection()
    kafka_stream_df = connect_to_kafka(spark)
    cassandra_session = create_cassandra_connection()

    if spark and kafka_stream_df and cassandra_session:
        create_keyspace(cassandra_session)
        create_table(cassandra_session)

        parsed_df = process_stream_data(kafka_stream_df)

        query = parsed_df.writeStream \
            .format("org.apache.spark.sql.cassandra") \
            .option("checkpointLocation", "/tmp/cisco_price_checkpoint") \
            .option("keyspace", "spark_streams") \
            .option("table", "cisco_prices") \
            .outputMode("append") \
            .start()

        query.awaitTermination()
