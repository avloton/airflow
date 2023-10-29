
import pendulum
from airflow.decorators import dag, task
from config import root, db_url, input_file

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["work"]
)
def my_etl():
    
    @task()
    def extract(file_name: str):
        import pandas as pd
                
        #Read Excel sheets
        transactions = pd.read_excel(f'{root}{file_name}', sheet_name='Transactions')
        customers = pd.read_excel(f'{root}{file_name}', sheet_name='CustomerDemographic')
        customers_address = pd.read_excel(f'{root}{file_name}', sheet_name='CustomerAddress')

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
        task.initialize_stg('STG_TRANSACTIONS')
        task.insert_data_to_stg('STG_TRANSACTIONS', d['transactions'])
        task.initialize_stg('STG_CUSTOMERS')
        task.insert_data_to_stg('STG_CUSTOMERS', d['customers'])
        task.initialize_stg('STG_CUSTOMERS_ADDRESS')
        task.insert_data_to_stg('STG_CUSTOMERS_ADDRESS', d['customers_address'])

        return task.db_path
    
    @task()
    def to_dwh(stage_db_path):
        from py_scripts.etl_task import EtlTask
        import logging

        #Specify path to DB
        task = EtlTask(stage_db_path)
        
        #Create DB functions
        logging.info("Create DB functions")
        task.make_dwh(f'{root}sql_scripts/create_DB_FUNCTIONS.sql')

        #Create DWH tables if not exists
        logging.info("Create DWH tables if not exists")
        task.make_dwh(f'{root}sql_scripts/create_DWH_CUSTOMERS.sql')
        task.make_dwh(f'{root}sql_scripts/create_DWH_PRODUCTS.sql')
        task.make_dwh(f'{root}sql_scripts/create_DWH_TRANSACTIONS.sql')

        #Create DB triggers
        logging.info("Create DB triggers")
        task.make_dwh(f'{root}sql_scripts/create_DB_TRIGGERS.sql')

        #Insert data to DWH
        logging.info("Insert data to DWH")
        task.make_dwh(f'{root}sql_scripts/insert_DWH_PRODUCTS.sql')
        task.make_dwh(f'{root}sql_scripts/insert_DWH_CUSTOMERS.sql')
        task.make_dwh(f'{root}sql_scripts/insert_DWH_TRANSACTIONS.sql')

        return 'to_dwh_ok'


    d = extract(input_file)
    stage_db_path = to_stage(db_url, d)
    to_dwh(stage_db_path)
    
my_etl()
