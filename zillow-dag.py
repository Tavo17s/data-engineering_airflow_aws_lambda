import json
import requests

from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

with open('/home/ubuntu/airflow/config_api.json', 'r') as config_file:
    api_host_key = json.load(config_file)

now = datetime.now()
dt_now_string = now.strftime("%Y-%m-%d_%H-%M-%S")

s3_bucket = 'cleaned-data-csv-zillow-bucket'

def extract_zillow_data(**kwargs):
    url = kwargs['url']
    headers = kwargs['headers']
    querystring = kwargs['querystring']
    dt_string = kwargs['date_string']

    response = requests.get(url, headers=headers, params=querystring)
    response_data = response.json()

    output_file_path = f"/home/ubuntu/response_data_{dt_string}.json"
    file_str = f'response_data_{dt_string}.csv'

    with open(output_file_path, "w") as output_file:
        json.dump(response_data, output_file, indent=4)
    output_list = [output_file_path, file_str]

    return output_list


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 24),
    'email': ['myemail@domain.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG('zillow_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    extract_zillow_data_var = PythonOperator(
        task_id = 'tsk_extract_zillow_data_var',
        python_callable = extract_zillow_data,
        op_kwargs = {'url': "https://zillow56.p.rapidapi.com/search",
            'querystring': {"location":"houston, tx","output":"json","status":"forSale","sortSelection":"priorityscore","listing_type":"by_agent","doz":"any"},
            'headers': api_host_key,
            'date_string': dt_now_string
        }
    )

    load_to_s3 = BashOperator(
        task_id = 'tsk_load_to_S3',
        bash_command = 'aws s3 mv {{ ti.xcom_pull("tsk_extract_zillow_data_var")[0]}} s3://zillow-pipeline-bucket/ ',
    )

    is_file_in_s3_available = S3KeySensor(
        task_id='tsk_is_file_in_s3_available',
        bucket_key='{{ ti.xcom_pull("tsk_extract_zillow_data_var")[1]}}',
        bucket_name=s3_bucket,
        aws_conn_id='aws_s3_conn',
        wildcard_match=False,
        timeout=60,
        poke_interval=5,
    )


    extract_zillow_data_var >> load_to_s3 >> is_file_in_s3_available