#Library imports
from airflow import DAG
import datetime as datetime
from datetime import timedelta
from airflow.operators.bash_operator import bash_operator
from airflow.utils.dates import days_ago

#Task 1.1 - Define DAG Arguments
default_args = {
    'owner' : 'Phoebe Clark',
    'start_date' : days_ago(0),
    'email': ['phoebenclark@gmail.com'],
    'email_on_failure' :True,
    'email_on_retry' :True,
    'retries' : 1,
    'retry_delay' : dt.timedelta(minutes=5)
}

#Task 1.2
dag = DAG(
    'ETL_toll_data',
    description='Apache Airflow Final Assignment',
    default_args=default_args,
    schedule_interval=dt.timedelta(days=1)
)

#Task 1.3 - Create task to unzip data
unzip_data = BashOperator(
    task_id = 'unzip_data',
    bash_command='tar -xzf /home/project/airflow/dags/finalassignment/tolldata.tgz',
    dag=dag,
)

#Task 1.4 - Create a task to extract data from csv file
extract_data_from_csv = BashOperator(
    task_id = 'extract_data_from_csv',
    bash_command = 'cut -d"," -f1-4 vehicle_data.csv > csv_data.csv',
    dag=dag
)

#Task 1.5 - Create a task to extract data from tsv file
extract_data_from_tsv = BashOperator(
    task_id = 'extract_data_from_tsv',
    bash_command = 'cut -f5-7 tollplaza.tsv > tsv_data.csv',
    dag=dag,
)

#Task 1.6 - Create a task to extract data from fixed width file
extract_data_from_fixed_width = BashOperator(
    task_id = 'extract_data_from_fixed_width',
    bash_command = 'cut -c59-68 payment-data.txt | tr " " "," > fixed_width_data.csv',
    dag=dag
)

#Task 1.7 -  Create a task to consolidate data extracted from previous tasks
consolidate_data = BashOperator(
    task_id = 'consolidate_data',
    bash_command = 'paste csv_data.csv tsv_data.csv fixed_width_data > extracted_data.csv',
    dag=dag,
)

#Task 1.8 - Transform and load the data
transform_data = BashOperator(
    task_id = 'transform_data',
    bash_command = 'awk $5=toupper($5) < extracted_data.csv > transformed_data.csv',
    dag=dag,
)

#Task 1.9 - Define the task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data

