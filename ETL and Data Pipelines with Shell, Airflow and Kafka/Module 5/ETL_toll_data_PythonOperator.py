from airflow.models import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os


def create_directories():
    dirs_to_create = [SOURCE_DIR, EXTRACTED_DIR, TRANSFORMED_DIR]

    for d_path in dirs_to_create:
        try:
            os.makedirs(d_path, exist_ok=True)
            if os.path.exists(d_path):
                print(f"Verified directory exists: {d_path}")
            else:
                print(f"ERROR: Directory did not exist after creation attempt: {d_path}")
        except Exception as e:
            raise

def download_data():
    import urllib.request

    try:
        req = urllib.request.Request(tarfile_url)
        req.add_header('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.88 Safari/537.36')

        urllib.request.urlretrieve(req.full_url, tarfile_path)
        print("\nDownload complete.")
    except Exception as e:
        raise


def unzip_data():
    import tarfile

    if not os.path.exists(extraction_path):
        os.makedirs(extraction_path)
    try:
        with tarfile.open(tarfile_path, "r:gz") as tar:
            tar.extractall(path=extraction_path)
    except tarfile.ReadError:
        print(f"Error: Could not read '{tarfile_path}'.")
    except FileNotFoundError:
        print(f"Error: The file '{tarfile_path}' was not found.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def extract_data_from_csv():
    df = pd.read_csv(vehicle_data, header = None)
    df = df.iloc[:,0:4]
    df.columns =  ["row_id", "timestamp", "anon_vehicle_num", "vehicle_type"]
    df.to_csv(csv_data, index = False)


def extract_data_from_tsv():
    df = pd.read_csv(tollplaza_data, sep="\t", header=None)
    df = df.iloc[:,-3:]
    df.columns = ["num_axles", "tollplaza_id", "tollplaza_code"]
    df.to_csv(tsv_data, index = False)


def extract_data_from_fixed_width():
    df = pd.read_fwf(payment_data, header=None)
    df = df.iloc[:, -2:]
    df.columns = ["payment_type_code", "vehicle_code"]
    df.to_csv(fixed_width_data, index=False)

def consolidate_data():
    df1 = pd.read_csv(csv_data)
    df2 = pd.read_csv(tsv_data)
    df3 = pd.read_csv(fixed_width_data)
    df = pd.concat([df1,df2,df3], axis=1)
    df.to_csv(extracted_data, index=False)

def transform_data():
    df = pd.read_csv(extracted_data)

    df["vehicle_type"] = df["vehicle_type"].astype(str).str.upper()

    df.to_csv(transformed_data, index = False)

def load_data():
    import mysql.connector
    import sqlalchemy

    engine = sqlalchemy.create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{database}')

    df = pd.read_csv(transformed_data)

    df.to_sql('tolldata', engine, if_exists='replace', index=False)


BASE_DIR = os.path.dirname(os.path.abspath(__file__))

SOURCE_DIR = os.path.join(BASE_DIR, 'Data', 'Source')
EXTRACTED_DIR = os.path.join(BASE_DIR, 'Data', 'Extracted')
TRANSFORMED_DIR = os.path.join(BASE_DIR, 'Data', 'Transformed')

tarfile_url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz"
tarfile_path = os.path.join(SOURCE_DIR, "tolldata.tgz")
extraction_path = SOURCE_DIR

vehicle_data = os.path.join(SOURCE_DIR, "vehicle-data.csv")
tollplaza_data = os.path.join(SOURCE_DIR, "tollplaza-data.tsv")
payment_data = os.path.join(SOURCE_DIR, "payment-data.txt")

csv_data = os.path.join(EXTRACTED_DIR, "csv_data.csv")
tsv_data = os.path.join(EXTRACTED_DIR, "tsv_data.csv")
fixed_width_data = os.path.join(EXTRACTED_DIR, "fixed_width_data.csv")
extracted_data = os.path.join(EXTRACTED_DIR, "extracted_data.csv")

transformed_data = os.path.join(TRANSFORMED_DIR, "transformed_data.csv")

host = "host.docker.internal"
user = "root"
password = "admin"
database = "airflowdb"


default_args = {
    'owner' : 'Sukrut',
    'email' : 'iamsukrut.sb@gmai.com',
    'email_on_failure' : True,
    'email_on_retry' : True,
    'retries' : 0, # Consider setting retries to 0 for initial debugging of duplicates
    'retry_delay' : timedelta(minutes=5),
    'start_date' : datetime(2025,6,16)
}

dag = DAG(
    dag_id = 'ETL_toll_data',
    description = 'Apache Airflow Final Assignment',
    default_args = default_args,
    schedule = timedelta(minutes=1),
    catchup=False,
    max_active_runs=1
)

create_directories_task = PythonOperator(
    task_id='create_directories',
    python_callable=create_directories,
    dag = dag
)

download_data_task = PythonOperator(
    task_id="download_data",
    python_callable=download_data,
    dag = dag
)

unzip_data_task = PythonOperator(
    task_id='unzip_data',
    python_callable=unzip_data,
    dag=dag
)

extract_data_from_csv_task = PythonOperator (
    task_id='extract_data_from_csv',
    python_callable=extract_data_from_csv,
    dag=dag
)

extract_data_from_tsv_task = PythonOperator(
    task_id='extract_data_from_tsv',
    python_callable=extract_data_from_tsv,
    dag=dag
)

extract_data_from_fixed_width_task = PythonOperator(
    task_id='extract_data_from_fixed_width',
    python_callable=extract_data_from_fixed_width,
    dag=dag
)

consolidate_data_task = PythonOperator(
    task_id='consolidate_data',
    python_callable=consolidate_data,
    dag=dag
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag
)

create_directories_task >> download_data_task >> unzip_data_task

unzip_data_task >> [extract_data_from_csv_task, extract_data_from_tsv_task, extract_data_from_fixed_width_task]

[extract_data_from_csv_task, extract_data_from_tsv_task, extract_data_from_fixed_width_task] >> consolidate_data_task

consolidate_data_task >> transform_data_task >> load_data_task