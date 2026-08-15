from config import SparkConfig
from streaming import Streaming




if __name__ == "__main__":

    spark_config = SparkConfig(
        app_name = "EneryRealTimeStreaming",
        minio_endpoint="http://localhost:9000",
        minio_access_key="_",
        minio_secret_key="_"
    )

    spark_session = spark_config.create_spark_session()


    stream = Streaming(
        spark=spark_session,
        kafka_serv="localhost:9092",
        kafka_tp="test",
        minio_path="s3a://bronze/energy_data/",
        minio_checkpoint="s3a://bronze/checkpoints/energy_data/"
    )

    stream.run()