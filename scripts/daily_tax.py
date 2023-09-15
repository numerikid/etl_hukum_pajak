from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from src.mains import ddtc_ikpi_hukumonline,ibfd_itr_bprd_belasting,dateFormat
from src.ingestDB import load_to_dwh,createLogs_table,load_logs_dwh


path_result_all = Variable.get('RESULT_DAILY')
date_now = dateFormat()
default_args = {
    'owner_email': 'admin@admin.com',
    'depends_on_past': False,
    'retries': 1,
    'timezone': 'Asia/Jakarta',
    'retry_delay': timedelta(minutes=3)

}

with DAG(
    'TaxTester_Daily',
    description='ETLs daily to Database',
    start_date = datetime(2023,9,14),
    schedule_interval='0 09 * * *',
    default_args = default_args,
) as dag:

    start_task = DummyOperator(task_id = 'Start_Task',)

    # Scrapping Process
    ddtc_ikpi_hukumonline_operator = PythonOperator(
        task_id='ScrapeFrom_DDTC_Ikpi_Hukumonline',
        python_callable=ddtc_ikpi_hukumonline,
        op_args=['daily', path_result_all],
        dag=dag,)
    
    ibfd_itr_bprd_belasting_operator = PythonOperator(
        task_id='ScrapeFrom_Ibfd_Itr_Bprd_Belasting',
        python_callable=ibfd_itr_bprd_belasting,
        op_args=['daily', path_result_all],
        dag=dag,)
    #============================================================================
    LoadDatatoDB = PythonOperator(
        task_id = 'LoadDataToDB',
        python_callable = load_to_dwh,
        op_args=['daily',path_result_all],
        dag= dag,)
    
    CreateLogTable = PythonOperator(
        task_id = 'CreateLogsTable',
        python_callable = createLogs_table,
        dag= dag,)
    
    LoadLogstoDB = PythonOperator(
        task_id = 'LoadLogsToDB',
        python_callable = load_logs_dwh,
        op_args=[date_now],
        dag= dag,)

    end_task = DummyOperator(task_id = 'End_Task',)

    start_task >> [ddtc_ikpi_hukumonline_operator,ibfd_itr_bprd_belasting_operator] >> LoadDatatoDB >> CreateLogTable >> LoadLogstoDB >> end_task