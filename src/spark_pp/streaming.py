from config import SparkConfig
from schemas import DataSchema
from transformations import Transformations
from pyspark.sql import DataFrame, SparkSession


class Streaming:

    def __init__(self, spark: SparkSession, kafka_serv: str, kafka_tp: str, minio_path: str, minio_checkpoint: str):

        self.spark = spark
        self.kafka_serv = kafka_serv
        self.kafka_tp = kafka_tp
        self.kafka_tp = kafka_tp
        self.minio_path =  minio_path
        self. minio_checkpoint =  minio_checkpoint



    def run(self):

        df = self.spark.readStream.format("kafka").load()

        schema = DataSchema.get_kafka_payload_schema()
        transformer = Transformations()

        df = transformer.transform(df, schema)

        query = df.writeStream.format("parquet").start()

        query.awaitTarmination()