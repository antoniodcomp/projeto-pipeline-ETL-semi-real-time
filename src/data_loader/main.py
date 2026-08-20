from config import DataLoaderConfig
from extract import Extract
from load import LoadToWareHouse


def main():

    config = DataLoaderConfig()

    extract = Extract(bucket='datalake', prefix="raw/uci/data.parquet", config=config)

    dataFrame = extract.extract_to_dataFrame()

    engine = config.get_postgres_engine()

    loader = LoadToWareHouse(df=dataFrame, engine=engine, sql_path="sql/bronze/sensor_uci.sql")

    loader.create_table()
    loader.insert_to_dataWarehouse()


if __name__ == "__main__":
    main()