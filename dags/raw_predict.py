import requests
import json
import boto3
from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.empty_operator import EmptyOperator
# from airflow.operators.python_operator import PythonOperator
# from airflow.operators.dummy_operator import DummyOperator
from json_scraper import json_scraper

default_args = {
  'owner': 'mourtallah',
  'depends_on_past': 'True',
  'start_date': datetime(2022, 11, 1),
  'email': ['serigne67@gmail.com','serigne.faye@icloud.com'],
  'email_on_failure': True,
  'email_on_retry': True,
  'retries': 1,
  'retry_delay': timedelta(minutes=15),
  'queue': 'bash_queue',
  'pool': 'backfill',
  'priority_weight': 10,
  'end_date': datetime(2022, 12, 30),
}


json_scraper('https://www.predictit.org/api/marketdata/all', '../predict_market.json', 'data-mbfr')


with DAG(
  dag_id='raw_predictit',
  default_args=default_args,
  description='Scrape PredictIt data',
  schedule_interval=datetime.timedelta(days=1),
  start_date=default_args['start_date'],
  catchup=False,
  tags=['predictit', 'scrape','sdg'],
) as dag:
  extract_predictit = PythonOperator(
    task_id='extract_predictit',
    pythion_callable=json_scraper,
    op_kwargs={'url': 'https://www.predictit.org/api/marketdata/all'
               'file_name': 'predict_market.json',
               'bucket': 'data-mbfr'},
    dag=dag
  )
  
  ready = DummyOperator(task_id='ready')
  
  extract_predictit >> ready
