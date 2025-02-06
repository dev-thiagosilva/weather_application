from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from weather_application import collect_data, process_data, data_partitioning

dag = DAG('weather_app', description='Dag related to extracting data from weather apis.',
          schedule_interval='@once', start_date=datetime(2024, 10, 1), catchup=False)

start = EmptyOperator(task_id='start', dag=dag)
collect_data = PythonOperator(task_id='collect_data', python_callable=collect_data, dag=dag)
process_data = PythonOperator(task_id='process_data', python_callable=process_data, dag=dag)
data_partitioning = PythonOperator(task_id='data_partitioning', python_callable=data_partitioning, dag=dag)

start >> collect_data >> process_data >> data_partitioning
