from airflow.models import DAG
from airflow.providers.standard.operators.python import PythonOperator
import datetime
import pandas as pd
import tarfile
import os


tarfile_path = "H://IBM-Data-Engineer/ETL and Data Pipelines with Shell, Airflow and Kafka/Module 5/Data/tolldata.tgz"
extraction_path = "H://IBM-Data-Engineer/ETL and Data Pipelines with Shell, Airflow and Kafka/Module 5/Data"
vehicle_data_csv = "H://IBM-Data-Engineer/ETL and Data Pipelines with Shell, Airflow and Kafka/Module 5/Data/vehicle-data.csv"
tollplaza_data_tsv = "H://IBM-Data-Engineer/ETL and Data Pipelines with Shell, Airflow and Kafka/Module 5/Data/tollplaza-data.tsv"
payment_data_txt = "H://IBM-Data-Engineer/ETL and Data Pipelines with Shell, Airflow and Kafka/Module 5/Data/payment-data.txt"

def unzip_data(tgz_path, extract_dir):
    if not os.path.exists(extract_dir):
        os.makedirs(extract_dir)
    try:
        with tarfile.open(tgz_path, "r:gz") as tar:
            tar.extractall(path=extract_dir)
    except tarfile.ReadError:
        print(f"Error: Could not read '{tgz_path}'.")
    except FileNotFoundError:
        print(f"Error: The file '{tgz_path}' was not found.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def extract_data_from_csv(csv_path):
    df = pd.read_csv(csv_path)
    print(df.head)

extract_data_from_csv(vehicle_data_csv)
# default_args = {
#     'owner': 'Sukrut',
#     'start_date': datetime.date.today(),
#     'email': 'sukrutbhoite@gmail.com',
#     'email_on_failure': True,
#     'email_on_retry': True,
#     'retries': 1,
#     'retry_delay': datetime.timedelta(minutes=5),
# }
#
# dag = DAG(
#     'ETL_toll_data',
#     schedule_interval=datetime.timedelta(days=1),
#     default_args=default_args,
#     description='Apache Airflow Final Assignment'
# )
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


