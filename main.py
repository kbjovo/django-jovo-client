from confluent_kafka import Consumer, KafkaException
import json

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'cdc-python-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['dbserver1.public.customers'])

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        
        value = json.loads(msg.value().decode('utf-8'))
        op = value['op']
        after = value['after']
        before = value['before']

        if op == 'c':   # insert
            print(f"INSERT: {after}")
        elif op == 'u': # update
            print(f"UPDATE: before={before}, after={after}")
        elif op == 'd': # delete
            print(f"DELETE: {before}")
        elif op == 'r': # snapshot
            print(f"SNAPSHOT: {after}")
            

finally:
    consumer.close()
