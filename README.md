# Airflow project


### How to start

    ~/airflow$ AIRFLOW_UID=1000 docker compose up -d

AIRFLOW_UID - this is your user id.


#### DB Postgresql (data)

    postgresql://admin:admin@localhost:5432/db


#### Airflow Web UI

    http://localhost:8080/
    login and password: airflow

#### Datalens Web UI 

    http://localhost:8083/

Datalens GitHub page: https://github.com/datalens-tech/datalens



#### Minio S3 Web UI

    http://localhost:9001/
    login: admin
    password: sample_key

#### MLFlow WebUI

    http://localhost:5001/