from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

# Import ETL function
from etl_scripts.transform_load import run_etl_process

with DAG(
    dag_id="transjakarta_etl_pipeline",
    start_date=pendulum.datetime(2025, 10, 19, tz="Asia/Jakarta"),
    # Jadwal: Setiap hari pada pukul 7 pagi
    schedule="0 0 * * *", 
    catchup=False,
    tags=["etl", "transjakarta", "daily"],
) as dag:
    
    etl_task = PythonOperator(
        task_id="run_full_etl_process",
        python_callable=run_etl_process,
        execution_timeout=pendulum.duration(minutes=10), 
    ) 