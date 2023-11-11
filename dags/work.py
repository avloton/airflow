
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

        return task.db_path
    
    @task()
    def prepare_data_for_ml(db_path):
        from py_scripts.etl_task import EtlTask
        import logging

        logging.info("Preparing ML Data ...")
        task = EtlTask(db_path)
        task.read_sql(f'{root}sql_scripts/create_ML_DATA.sql')
        task.read_sql(f'{root}sql_scripts/clean_ML_DATA.sql')
        task.read_sql(f'{root}sql_scripts/insert_ML_DATA.sql')
        logging.info("Done.")

        return task.db_path
    
    @task()
    def transform_ml_data(db_path):
        from py_scripts.etl_task import EtlTask
        import pandas as pd

        #Read ML data
        task = EtlTask(db_path)
        sqlalchemy_engine = task.get_sqlalchemy_conn()
        query = "select * from ml_data"
        df = pd.read_sql_query(query, con=sqlalchemy_engine)

        #Transform ML data
        df = pd.get_dummies(data=df, columns=['wealth_segment'])
        df = pd.get_dummies(data=df, columns=['job_industry_category'])
        df.replace({False: 0, True: 1}, inplace=True)

        #Write ML data to DB
        df.to_sql('ml_data_transformed', con=sqlalchemy_engine, if_exists='replace')

        return db_path
    
    @task()
    def create_model(db_path):
        from py_scripts.etl_task import EtlTask
        from py_scripts.model import Model
        from sklearn.model_selection import train_test_split
        import pandas as pd
        import os
        import mlflow
        from mlflow.models import infer_signature

        #Read ML data
        task = EtlTask(db_path)
        sqlalchemy_engine = task.get_sqlalchemy_conn()
        query = "select * from ml_data_transformed"
        df = pd.read_sql_query(query, con=sqlalchemy_engine)

        #Initialize Model and X, y
        model = Model()
        X = df.drop(columns=['online_order', 'index'], axis=1)
        y = df['online_order']
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.4, random_state=0)

        #MLFlow experiment name
        os.environ['MLFLOW_EXPERIMENT_NAME'] = 'MyExp' 

        with mlflow.start_run() as run:
            #Create Model
            model.fit(X_train, y_train)
            preds = model.predict(X_test)
            signature = infer_signature(X_train, preds)
            mlflow.sklearn.log_model(model.model, "MyModel", signature=signature, registered_model_name='RandomForestModel')

            #Send Metrics to MLFlow
            metrics = model.get_metrics(X_test, y_test)
            for j in metrics:
                mlflow.log_metric(j, metrics[j])

        return 'Done'


    #Etl file to DB
    d = extract(input_file)
    stage_db_path = to_stage(db_url, d)
    dwh_db_path = to_dwh(stage_db_path)

    #Etl web data to DB
    web_data = extract_web_data(web_data_category)
    web_data_to_db(db_url, web_data)

    #ML Operations
    pdfm = prepare_data_for_ml(dwh_db_path)
    tmd = transform_ml_data(pdfm)
    create_model(tmd)
    
my_etl()
