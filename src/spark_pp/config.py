from pyspark import SparkSession

class SparkConfig:

    def __init__(self, minio_name: str, minio_secret_key: str, minio_acess_key: str, minio_endpoint: str):

        self.minio_name = minio_name
        self.minio_secret_key = minio_secret_key
        self.minio_acess_key = minio_acess_key
        self.minio_endpoint = minio_endpoint

    def create_spark_session(self) -> SparkSession:

        spark = SparkSession.builder \
            .appName("Spark-MinIO-Connection") \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
            .config("spark.hadoop.fs.s3a.endpoint", self.minio_endpoint) \
            .config("spark.hadoop.fs.s3a.access.key", self.minio_acess_key) \
            .config("spark.hadoop.fs.s3a.secret.key", self.minio_secret_key) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .getOrCreate()

        return spark