from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

class DataSchema:

    @staticmethod
    def get_kafka_payload_schema() -> StructType:

        struct = StructType([
        StructField("house_id", StringType(), True),
        StructField("current_timestamp", TimestampType(), True),
        StructField("original_datetime", StringType(), True),
        StructField("global_active_power", DoubleType(), True),
        StructField("global_reactive_power", DoubleType(), True),
        StructField("voltage", DoubleType(), True),
        StructField("Global_intensity", DoubleType(), True),
        StructField("Sub_metering_1", DoubleType(), True),
        StructField("Sub_metering_2", DoubleType(), True),
        StructField("Sub_metering_3", DoubleType(), True)
        ])

        return struct


