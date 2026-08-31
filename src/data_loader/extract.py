import pandas as pd
import s3fs
from config import DataLoaderConfig


class Extract:

    def __init__(self, bucket: str, prefix: str, config: DataLoaderConfig):
        self.bucket = bucket
        self.prefix = prefix
        self.fs = config.get_s3_filesystem


    def extract_to_dataFrame(self) -> pd.DataFrame:


        path = f"{self.bucket}/{self.prefix}"


        df = pd.read_parquet(path, filesystem=self.fs)

        return df