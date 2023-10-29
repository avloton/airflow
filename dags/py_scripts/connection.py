import pandas as pd
import psycopg2
import logging

class Connection:

    def __init__(self, conn_path) -> None:
        self.conn_path = conn_path
        
    @staticmethod
    def open_file(file_path: str) -> str:
        with open(file_path, 'r') as f:
            file = f.read()
        return file

    def read_sql_script(self, sql_path: str, file_script: bool = True) -> None:
        """
        """
        conn = psycopg2.connect(self.conn_path)
        cur = conn.cursor()
        try:
            if file_script:
                sql = self.open_file(sql_path)
            else:
                sql = sql_path
            cur.execute(sql)
            conn.commit()
        except Exception as error:
            print(error)
        finally:
            conn.close()

    def insert_data(self, table: str, data: pd.DataFrame) -> None:

        data = data.astype('str')

        query = f"""INSERT INTO {table}({",".join(list(data))})
                VALUES ({",".join(['%s' for x in range(data.shape[1])])})"""
        
        conn = psycopg2.connect(self.conn_path)
        cur = conn.cursor()

        try:
            cur.executemany(query, list(data.to_records(index=False)))
            conn.commit()
        except Exception as error:
            print(error)
        finally:
            conn.close()
