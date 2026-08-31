import os
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
import s3fs


class DataLoaderConfig:


    def __init__(self):

        self.pg_user = os.getenv("POSTGRES_USER", "admin")
        self.pg_password = os.getenv("POSTGRES_PASSWORD", "admin123")

        self.pg_host = os.getenv("POSTGRES_HOST", "postgres_dw") 
        self.pg_port = os.getenv("POSTGRES_PORT", "5432")
        self.pg_db = os.getenv("POSTGRES_DB", "data_warehouse")

        
        self.minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
        self.minio_access_key = os.getenv("MINIO_ACCESS_KEY", "admin")
        self.minio_secret_key = os.getenv("MINIO_SECRET_KEY", "admin123")


    def get_postgres_engine(self) -> Engine:
        db_url = f"postgresql+psycopg2://{self.pg_user}:{self.pg_password}@{self.pg_host}:{self.pg_port}/{self.pg_db}"
        engine = create_engine(db_url)

        return engine


    def get_s3_filesystem(self) -> s3fs.S3FileSystem:
        return s3fs.S3FileSystem(
            key=self.minio_access_key,
            secret=self.minio_secret_key,
            client_kwargs={'endpoint_url': self.minio_endpoint}
        )

        