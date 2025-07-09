from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from pymongo import MongoClient, UpdateOne
import json

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    dag_id='user_event_processing',
    default_args=default_args,
    start_date=datetime(2025, 7, 7),
    schedule_interval='*/2 * * * *' # 2 dakikada bir
)

KAFKA_TOPIC = 'UserEvents'
MONGO_URL = 'mongodb://localhost:27017'
MONGO_DB = 'event_db'
RAW_COLLECTION = 'user_events_raw'
SUMMARY_COLLECTION = 'user_event_summary'

def consume_user_events():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        server='localhost:9092',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id='airflow-consumer-group'
    )

    mongo = MongoClient(MONGO_URL)
    db = mongo[MONGO_DB]
    raw_col = db[RAW_COLLECTION]

    messages = []
    for message in consumer:
        messages.append(message.value)
        if len(messages) > 500:
            break

    if messages:
        raw_col.insert_many(messages)
    consumer.close()

def aggregate_user_events():
    mongo = MongoClient(MONGO_URL)
    db = mongo[MONGO_DB]
    raw_col = db[RAW_COLLECTION]
    summary_col = db[SUMMARY_COLLECTION]

    pipeline = [
        {
            "$group": {
                "id": {
                    "UserId": "$UserId",
                    "EventName": "$EventName"
                },
                "EventCount": {"$sum": 1}
            }
        }
    ]

    aggregation = list(raw_col.aggregate(pipeline))

    updated_values = []
    for doc in aggregation:
        filter_q = {
            "UserId": doc["id"]["UserId"],
            "EventName": doc["id"]["EventName"]
        }
        update_doc = {
            "$set": {
                "UserId": doc["id"]["UserId"],
                "EventName": doc["id"]["EventName"],
            },
            "$inc": {
                "EventCount": doc["EventCount"]
            }
        }
        updated_values.append(UpdateOne(filter_q, update_doc))

    if updated_values:
        summary_col.bulk_write(updated_values)

consume_task = PythonOperator(
    task_id='con_ins_user_events',
    python_callable=consume_user_events,
    dag=dag
)

aggregate_task = PythonOperator(
    task_id='aggregate_event_counts',
    python_callable=aggregate_user_events,
    dag=dag
)

consume_task >> aggregate_task