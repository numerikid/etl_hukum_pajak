from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from src.mains import ddtc_ikpi_hukumonline,ibfd_itr_bprd_belasting
from src.ingestDB import load_to_dwh,create_table


# path result > /home/user/dags/results/all (all adalah nama file csv nya)
# export RESULT_ALL = /path/bla/bla
path_result_all = Variable.get('RESULT_ALL')


default_args = {
    'owner_email': 'admin@admin.com',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}
with DAG(
    'TaxTes_news',
    description='ETL Tax News to Database',
    start_date = datetime(2023,9,13),
    schedule_interval = None,
    default_args = default_args,
) as dag:

    start_task = DummyOperator(task_id = 'Mulai_Task',)

    ddtc_ikpi_hukumonline_operator = PythonOperator(
         task_id='ScrapeFrom_DDTC_Ikpi_Hukumonline',
         python_callable=ddtc_ikpi_hukumonline,
         op_args=['all', path_result_all],
         dag=dag,)
    
    ibfd_itr_bprd_belasting_operator = PythonOperator(
        task_id='ScrapeFrom_Ibfd_Itr_bprd_belasting',
        python_callable=ibfd_itr_bprd_belasting,
        op_args=['all', path_result_all],
        dag=dag,)
    

    # Database Processing
    CreateTableDB = PythonOperator(
        task_id = 'CreateTable',
        python_callable = create_table,
        dag= dag,)

    LoadDatatoDB = PythonOperator(
        task_id = 'LoadDataToDB',
        python_callable = load_to_dwh,
        op_args=['all',path_result_all],
        dag= dag,)

    end_task = DummyOperator(task_id = 'Task_Selesai',)

    start_task >> [ddtc_ikpi_hukumonline_operator,ibfd_itr_bprd_belasting_operator] >> CreateTableDB >> LoadDatatoDB >> end_task
