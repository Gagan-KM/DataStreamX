from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer
import time
import json
import requests
import logging
import warnings
warnings.filterwarnings('ignore')

#default args
default_args = {
    'owner' : "gagan",
    'start_date' : datetime(2023, 9, 22, 11, 00)
}

def get_data():
    path = r"https://randomuser.me/api/"
    res = requests.get(path)
    res = res.json()
    res = res['results'][0]
    return res

def format_data(res):
    data = {}
    # Extracting relevant fields
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{res['location']['street']['number']} {res['location']['street']['name']}, {res['location']['city']}, {res['location']['state']}, {res['location']['country']}"
    data['postcode'] = res['location']['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['large'] 
    return data


def stream_data():
    res = get_data()
    res = format_data(res)
    
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60:
            break
        try:
            res = get_data()
            res = format_data(res)
            producer.send('user_created', json.dumps(res).encode('utf-8'))
        except Exception as e:
            logging.error(f'An Error Occured {e}')
            continue

#entry point
with DAG('user_automation',
        default_args = default_args,
        schedule_interval = '@daily',
        catchup = False) as dag:
    
    streaming_task = PythonOperator(
        task_id = 'stream_data_from_api',
        python_callable = stream_data
    )

#stream_data()