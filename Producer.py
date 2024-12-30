from confluent_kafka import Producer
import json
import time
import random

producer_config = {
    'bootstrap.servers': '127.0.0.1:9092',
    'client.id': 'python-producer'
}

producer = Producer(producer_config)
topic = 'stream_credits'

offset_file = '/run/media/nngiaminh1812/Data/ODAP/Final/offset_producer.txt'
def read_last_offset():
    with open(offset_file, 'r') as f:
        return int(f.read().strip())
    
    return 0

def write_last_offset(offset):
    with open(offset_file, 'w') as f:
        f.write(str(offset))


headers = ["User", "Card", "Year", "Month", "Day", "Time", "Amount", "Use Chip", "Merchant Name", "Merchant City", "Merchant State", "Zip", "MCC", "Errors?", "Is Fraud?"]

def to_dict(headers, data):
    values = data.strip().split(',')
    return dict(zip(headers, values))

with open('/run/media/nngiaminh1812/Data/ODAP/Final/credit_card_transactions-ibm_v2.csv', 'r') as file:
    current_offset = read_last_offset()
    rows = file.readlines()

    for i in range(current_offset, len(rows)):
        data = rows[i]
        data = to_dict(headers, data)
        producer.produce(topic, key=str(i), value=json.dumps(data))
        print(data)
        producer.flush()
        
        write_last_offset(i+1)
        time.sleep(random.randint(1, 3))