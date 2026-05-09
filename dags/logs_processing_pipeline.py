from datetime import datetime , timedelta
from airflow.operators.python import PythonOperator
from airflow import DAG
from confluent_kafka import Consumer, KafkaException
import boto3
import logging
from utils import get_secret
import re
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from confluent_kafka import Consumer, KafkaException
logger =logging.getLogger(__name__)

def parse_log_entry(log_entry):
    log_pattern = r'^(?P<ip>\d{1,3}(?:\.\d{1,3}){3}) - - \[(?P<timestamp>[^\]]+)\]"(?P<method>\w+) (?P<endpoint>[^\s]+) HTTP/1\.1" (?P<status>\d{3})(?P<size>\d+)"(?P<referrer>[^"]*)" (?P<user_agent>.+)$'
    match = re.match(log_pattern, log_entry)
    if not match:
        logger.warning(f"Invalid log format: {log_entry}")
        return None
    data = match.groupdict()
    data['status'] = int(data['status'])
    data['size'] = int(data['size'])
    try:
        parsed_timestamp = datetime.strptime(data['timestamp'], '%b %d %Y,%H:%M:%S')
        data['@timestamp'] = parsed_timestamp.isoformat()
    except ValueError:
        logger.error(f"Timestamp parsing error: {data['timestamp']}")
        return None
    return data


def consume_index_logs():
    secrets=get_secret("MWAA_Secrets_V3")
    consumer_config={
        'bootstrap.servers':secrets["KAFKA_BOOTSTRAP_SERVER"],
        'security.protocol':'SASL_SSL',
        'sasl.mechanisms':'PLAIN',
        'sasl.username': secrets['KAFKA_SASL_USERNAME'],
        'sasl.password': secrets['KAFKA_SASL_PASSWORD'],
        
        'group.id':'mwaa_log_indexer',
        'auto.offset.reset':'latest'
    }
    
    print("ES URL:", secrets['ELASTIC_SEARCH_URL'])
    es_config={
        'hosts':"https://my-elasticsearch-project-ec8921.es.us-central1.gcp.elastic.cloud:443",
        'api_key':secrets['ELASTIC_SEARCH_API_KEY']
          
        }
    consumer=Consumer(consumer_config)
    es= Elasticsearch(
    hosts=secrets['ELASTIC_SEARCH_URL'],
    api_key=secrets['ELASTIC_SEARCH_API_KEY']) 
    topic='billion_website_logs'
    consumer.subscribe([topic])
    print("Connected")
    try: 
        index_name='billion_website_logs_index'
        if not es.indices.exists(index=index_name):
            es.indices.create(index=index_name)
            logger.info(f'created index: {index_name}')
        
    except Exception as e:
        logger.error(f'Failed to create index: {index_name} {e}')
    try : 
        logs =[]
        while True:
            msg =consumer.poll(timeout=1.0)
            if msg is None: 
                break
            if msg.error():
                if msg.error().code()== KafkaException._PARTITION_EOF:
                    break
                raise KafkaException(msg.error())
            
            log_entry=msg.value().decode('utf-8')
            parsed_log=parse_log_entry(log_entry)
            if parsed_log:
               logs.append(parsed_log) 
            #index when 15000 logs are collected  
            if len(logs)>=15000:
               actions=[
                 {
                    '_op_type':'create',
                    '_index':index_name,
                    '_source':log
                }
                for log in logs
                ]
               success, failed=bulk(es,actions, refresh=True)
               logger.info(f'Indexed {success} logs, {len(failed)} failed')
               logs =[]
    except Exception as e:
        logger.error(f'Failed to index log:{e}')     
        
    #index any remaining logs
    try:
       if logs:
            actions=[
                {
                    '_op_type':'create',
                    '_index':index_name,
                    '_source':log
                }
                for log in logs
            ]
            bulk(es, actions, refresh=True)
            
    except Exception as e:
        logger.error(f'log processing error: {e}')
    finally:
        consumer.close()
        es.close()
        
 
 
#consume_index_logs()       

default_args={
    'owner':'Date Mastery Lab',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}
dag= DAG(
    dag_id='log_consumer_pipeline',
    default_args=default_args,
    description='Consume and index synthetic logs',
    schedule_interval='*/5 * * * *',
    start_date=datetime(2026,4,30),
    catchup=False,
    tags=['logs','kafka', 'production']
    
)


produce_logs_task= PythonOperator(
    task_id='generate_and_consume_logs',
    python_callable=consume_index_logs,
    dag=dag
)
