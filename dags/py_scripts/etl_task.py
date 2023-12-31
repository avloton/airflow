import pandas as pd
from py_scripts.connection import Connection
from config import root
from sqlalchemy import create_engine


class EtlTask:

    def __init__(self, db_path):
        self.db_path = db_path
    
    def read_sql(self, sql: str) -> None:
        connect = Connection(self.db_path)
        connect.read_sql_script(sql)
    
    def insert_data(self, table_name: str, data: pd.DataFrame) -> None:
        connect = Connection(self.db_path)
        connect.insert_data(table_name, data)

    def get_sqlalchemy_conn(self):
        engine = create_engine(self.db_path)
        return engine
