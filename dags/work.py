
import pendulum
from airflow.decorators import dag, task
from config import root, db_url, input_file, web_data_category

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["work"]
)
def my_etl():

    @task()
    def extract_web_data(category_name: str):
        from py_scripts.my_parser import MyParser

        parser = MyParser(category_name)
        data = parser.take_all_files()

        d = {'web_data': data}

        return d
    
    @task()
    def web_data_to_db(db_path: str, d: dict):
        from py_scripts.etl_task import EtlTask
        
        task = EtlTask(db_path)

        #Create table and insert data
        task.read_sql(f'{root}sql_scripts/create_WEB_DATA.sql')
        task.insert_data('WEB_DATA', d['web_data'])

        return 'ok'


    @task()
    def extract(file_name: str):
        import pandas as pd
                
        #Read Excel sheets
        transactions = pd.read_excel(f'{root}{file_name}', sheet_name='Transactions', na_filter=False)
        customers = pd.read_excel(f'{root}{file_name}', sheet_name='CustomerDemographic', na_filter=False)
        customers_address = pd.read_excel(f'{root}{file_name}', sheet_name='CustomerAddress', na_filter=False)

        # rename reserved word
        customers = customers.rename({'default': 'default_x'}, axis=1) 

        d = {'transactions': transactions, 'customers': customers, 'customers_address': customers_address}
        
        return d
    
    @task()
    def to_stage(db_path: str, d: dict):
        from py_scripts.etl_task import EtlTask

        #Specify path to DB
        task = EtlTask(db_path)

        #Initialize and insert data to STAGE
        task.read_sql(f'{root}sql_scripts/create_STG_TRANSACTIONS.sql')
        task.insert_data('STG_TRANSACTIONS', d['transactions'])

        task.read_sql(f'{root}sql_scripts/create_STG_CUSTOMERS.sql')
        task.insert_data('STG_CUSTOMERS', d['customers'])

        task.read_sql(f'{root}sql_scripts/create_STG_CUSTOMERS_ADDRESS.sql')
        task.insert_data('STG_CUSTOMERS_ADDRESS', d['customers_address'])

        return task.db_path
    
    @task()
    def to_dwh(stage_db_path):
        from py_scripts.etl_task import EtlTask
        import logging

        #Specify path to DB
        task = EtlTask(stage_db_path)
        
        #Create DB functions
        logging.info("Create DB functions")
        task.read_sql(f'{root}sql_scripts/create_DB_FUNCTIONS.sql')

        #Create DWH tables if not exists
        logging.info("Create DWH tables if not exists")
        task.read_sql(f'{root}sql_scripts/create_DWH_CUSTOMERS.sql')
        task.read_sql(f'{root}sql_scripts/create_DWH_PRODUCTS.sql')
        task.read_sql(f'{root}sql_scripts/create_DWH_TRANSACTIONS.sql')

        #Create DB triggers
        logging.info("Create DB triggers")
        task.read_sql(f'{root}sql_scripts/create_DB_TRIGGERS.sql')

        #Insert data to DWH
        logging.info("Insert data to DWH")
        task.read_sql(f'{root}sql_scripts/insert_DWH_PRODUCTS.sql')
        task.read_sql(f'{root}sql_scripts/insert_DWH_CUSTOMERS.sql')
        task.read_sql(f'{root}sql_scripts/insert_DWH_TRANSACTIONS.sql')

        return 'ok'

    #Etl file to DB
    d = extract(input_file)
    stage_db_path = to_stage(db_url, d)
    to_dwh(stage_db_path)

    #Etl web data to DB
    web_data = extract_web_data(web_data_category)
    web_data_to_db(db_url, web_data)
    
my_etl()
