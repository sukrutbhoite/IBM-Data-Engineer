from kafka import KafkaConsumer
from datetime import datetime
from sqlalchemy import create_engine
import pandas as pd
import json

BOOTSTRAP_SERVERS = ['127.0.0.1:9092']
TOPIC_NAME = 'datetime'
USER = "root"
PASSWORD = "admin"
HOST = "localhost"
DATABASE = "kafkadb"
TABLE = "datetime"

engine=create_engine(f'mysql+mysqlconnector://{USER}:{PASSWORD}@{HOST}/{DATABASE}')

consumer = KafkaConsumer(TOPIC_NAME, bootstrap_servers=BOOTSTRAP_SERVERS, value_deserializer=lambda x: json.loads(x.decode('utf-8')))
print(f"Starting to consume messages from '{TOPIC_NAME}' and write to '{TABLE}'...")
for message in consumer:
    print(f"Received: {message.value}")
    df = pd.DataFrame([message.value])
    df.to_sql(name=TABLE,con=engine, if_exists='append', index=False)









