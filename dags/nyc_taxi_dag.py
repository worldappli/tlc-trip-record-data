from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

with DAG (
    dag_id = "NYC_TAXI_DAG",
    start_date = datetime(2026,6,23),
    schedule = "*/15 * * * *",
    #schedule = None,
    catchup = False,
    default_args={
        "retries": 3
    }
) as dag :

    process =  BashOperator(
        task_id = "Process_Parquet",
        bash_command = "python /opt/airflow/scripts/parquet_batch_pep.py"
    )

    stats = BashOperator(
        task_id = "Statistik_Wert",
        bash_command = "python /opt/airflow/scripts/statistik_werte.py"
    )

    process >> stats