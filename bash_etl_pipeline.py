from datetime import timedelta
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'this_is_task',
    'start_date': days_ago(0),
    'email': ['email@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='ETL_toll_data',
    schedule_interval='@daily',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
)

base_path = "/airflow/dags/finalassignment/"

unzip_data = BashOperator(
    task_id="unzip_data",
    bash_command=f"""
        tar -zxvf {base_path}tolldata.tgz -C {base_path}
    """,
    dag=dag,
)

extract_data_from_csv = BashOperator(
    task_id="extract_data_from_csv",
    bash_command=f"""
    cut -d',' -f1,2,3,4 {base_path}vehicle-data.csv > {base_path}csv_data.csv
    """,
    dag=dag,
)

extract_data_from_tsv = BashOperator(
    task_id="extract_data_from_tsv",
    bash_command=f"""
    cut -d$'\t' -f5,6,7 {base_path}tollplaza-data.tsv | tr '\t' ',' > {base_path}tsv_data.csv
    """,
    dag=dag,
)

extract_data_from_fixed_width = BashOperator(
    task_id="extract_data_from_fixed_width",
    bash_command=f"""
    awk '{{print substr($0, 59, 3) "," substr($0, 63, 5)}}'  {base_path}payment-data.txt > {base_path}fixed_width_data.csv
    """,
    dag=dag,
)

consolidate_data = BashOperator(
    task_id="consolidate_data",
    bash_command=f"""
    paste -d',' {base_path}csv_data.csv {base_path}tsv_data.csv {base_path}fixed_width_data.csv > {base_path}extracted_data.csv
    """,
    dag=dag,
)

cleanup_data = BashOperator(
    task_id="cleanup_data",
    bash_command=f"""
    sed -i 's/\r//' {base_path}extracted_data.csv
    """,
    dag=dag,
)

transform_data = BashOperator(
    task_id="transform_data",
    bash_command=f"""
    awk 'BEGIN {{FS=OFS=","}} {{$4=toupper($4); print}}' {base_path}extracted_data.csv > {base_path}transformed_data.csv
    """,
    dag=dag,
)


unzip_data >> [extract_data_from_csv, extract_data_from_tsv, extract_data_from_fixed_width] >> consolidate_data >> cleanup_data >> transform_data
