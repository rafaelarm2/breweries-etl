"""Brewery pipeline Airflow DAG."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from brewery_etl.transformations.extract_brewery_data import extract_brewery_data
from brewery_etl.transformations.landing_to_bronze import landing_to_bronze
from brewery_etl.transformations.bronze_to_silver import bronze_to_silver
from brewery_etl.transformations.silver_to_gold import silver_to_gold

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'brewery_etl',
    default_args=default_args,
    description='Extract brewery data from API and process through medallion architecture',
    schedule='@daily',
    start_date=datetime(2025, 6, 8),
    catchup=False,
    tags=['brewery', 'etl', 'medallion'],
)

extract_task = PythonOperator(
    task_id='extract_brewery_data',
    python_callable=extract_brewery_data,
    dag=dag,
)

bronze_task = PythonOperator(
    task_id='landing_to_bronze',
    python_callable=landing_to_bronze,
    dag=dag,
)

silver_task = PythonOperator(
    task_id='bronze_to_silver',
    python_callable=bronze_to_silver,
    dag=dag,
)

gold_task = PythonOperator(
    task_id='silver_to_gold',
    python_callable=silver_to_gold,
    dag=dag,
)

extract_task >> bronze_task >> silver_task >> gold_task
