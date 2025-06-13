from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
from airflow.utils.dates import days_ago
import tarfile


tarfile_path = "H://tolldata.tgz"
extraction_path = "H://Temp"


def unzip(file,path):
    with tarfile.open(file, "r:gz") as tar:
        tar.extractall(path)

unzip(tarfile_path,extraction_path)
#
# default_args = {
#     'owner': 'Sukrut',
#     'start_date': days_ago(0),
#     'email': 'sukrutbhoite@gmail.com',
#     'email_on_failure': True,
#     'email_on_retry': True,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }
#
# dag = DAG(
#     'ETL_toll_data',
#     schedule_interval=timedelta(days=1),
#     default_args=default_args,
#     description='Apache Airflow Final Assignment'
# )
#
#
# unzip_data = BashOperator(
#     task_id='unzip_data',
#     bash_command='''tar -xzf /home/project/airflow/dags/finalassignment/tolldata.tgz \\
#                     -C /home/project/airflow/dags/finalassignment/''',
#     dag=dag
# )
#
# extract_data_from_csv = BashOperator(
#     task_id='extract_data_from_csv',
#     bash_command='''cut -d"," -f1-4 \\
#                     /home/project/airflow/dags/finalassignment/vehicle-data.csv \\
#                     > /home/project/airflow/dags/finalassignment/csv_data.csv''',
#     dag=dag
# )
#
# extract_data_from_tsv = BashOperator(
#     task_id='extract_data_from_tsv',
#     bash_command='''cut -f5-7 \\
#                     "/home/project/airflow/dags/finalassignment/tollplaza-data.tsv" \\
#                     | tr '\\t' ',' \\
#                     > "/home/project/airflow/dags/finalassignment/tsv_data.csv"''',
#     dag=dag
# )
#
# extract_data_from_fixed_width = BashOperator(
#     task_id='extract_data_from_fixed_width',
#     bash_command='''cat /home/project/airflow/dags/finalassignment/payment-data.txt \\
#                     | tr -s ' ' \\
#                     | cut -d ' ' -f 11,12 \\
#                     | tr ' ' ',' \\
#                     > /home/project/airflow/dags/finalassignment/fixed_width_data.csv''',
#     dag=dag
# )
#
# consolidate_data = BashOperator(
#     task_id='consolidate_data',
#     bash_command='''
#         paste -d',' \\
#             /home/project/airflow/dags/finalassignment/csv_data.csv \\
#             /home/project/airflow/dags/finalassignment/tsv_data.csv \\
#             /home/project/airflow/dags/finalassignment/fixed_width_data.csv \\
#         > /home/project/airflow/dags/finalassignment/extracted_data.csv
#     ''',
#     dag=dag
# )
#
# transform_data = BashOperator(
#     task_id='transform_data',
#     bash_command='''
#         paste -d',' \\
#             <(cut -d',' -f1-3 /home/project/airflow/dags/finalassignment/extracted_data.csv) \\
#             <(cut -d',' -f4 /home/project/airflow/dags/finalassignment/extracted_data.csv | tr '[:lower:]' '[:upper:]') \\
#             <(cut -d',' -f5- /home/project/airflow/dags/finalassignment/extracted_data.csv) \\
#         > /home/project/airflow/dags/finalassignment/transformed_data.csv
#     ''',
#     dag=dag
# )
#
#
# unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
#

