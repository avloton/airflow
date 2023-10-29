import pandas as pd
from py_scripts.connection import Connection
from config import root


class EtlTask:

    def __init__(self, db_path):
        self.db_path = db_path

    def initialize_stg(self, stg_table_name: str) -> None:
        connect = Connection(self.db_path)
        connect.read_sql_script(f"{root}sql_scripts/create_{stg_table_name}.sql")

    def insert_data_to_stg(self, stg_table_name: str, data: pd.DataFrame) -> None:
        connect = Connection(self.db_path)
        connect.insert_data(stg_table_name, data)

    def make_dwh(self, dwh_table_name: str) -> None:
        connect = Connection(self.db_path)
        connect.read_sql_script(dwh_table_name)



