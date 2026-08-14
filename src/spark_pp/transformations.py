from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.functions import (
    col,
    from_json,
    year,
    month,
    to_timestamp
)


class Transformations:

    def __init__(self):
        pass



    def transform(self, df_raw: DataFrame, schema: StructType) -> DataFrame:

        df_new = df_raw.withColum("data_json_transfomed", from_json(col("value").cast("string"), schema))

        df_new = df_new.select("data_json.*")

        df_f = df_new.withColumn("year", year(to_timestamp("original_datetime")))\
                     .withColumn("month", month(to_timestamp("original_datetime")))