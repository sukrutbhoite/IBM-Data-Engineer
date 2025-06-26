from kafka import KafkaProducer
from datetime import datetime
import json
import time

BOOTSTRAP_SERVERS = ['127.0.0.1:9092']
TOPIC_NAME = 'datetime'

def send_datetime_message():
    now = datetime.now()
    datetime_data = {
        'year': now.year,
        'month': now.month,
        'day': now.day,
        'hour': now.hour,
        'minute': now.minute,
        'second': now.second
    }

    producer.send(TOPIC_NAME, value=datetime_data)
    print(f"Sent: {datetime_data}")

producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS, value_serializer = lambda v: json.dumps(v).encode('utf-8'))

try:
    while True:
        send_datetime_message()
        time.sleep(10)
except KeyboardInterrupt:
    producer.flush()
    producer.close()
