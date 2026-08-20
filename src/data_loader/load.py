import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

class LoadToWareHouse:

    def __init__(self, df : pd.DataFrame, engine: Engine, sql_path: str):

        self.df = df
        self.engine = engine
        self.sql_path = sql_path


    def create_table(self):

        with open(self.sql_path, "r", encoding="urf-8") as f:

            sql = f.read()

        with self.engine.begin() as connection:
            connection.execute(text(sql))


    def insert_to_dataWarehouse(self):

        self.df.to_sql(
            "sensor_uci",
            con=self.engine,
            schema="bronze",
            if_exists="append",
            index=False
        )


