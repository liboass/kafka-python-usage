# -*- coding:utf-8 -*-

import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

from kafka import KafkaProducer
from kafka import KafkaConsumer

TOPIC = 'test'
producer_num = 1


def thread_producer():
    try:
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'], acks=1)
        for i in range(100):
            print(f"send msg id {i + 1}")
            producer.send(TOPIC, value=f"message id:{i + 1}".encode('utf-8'))
            time.sleep(1)
        producer.flush()
        producer.close()
    except Exception:
        traceback.print_exc()
    pass


def thread_consumer(consumer_id=0):
    try:
        # 第一种订阅方式
        # consumer = KafkaConsumer(TOPIC, group_id=TOPIC, bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest')
        # 第二种订阅方式
        consumer = KafkaConsumer(group_id=TOPIC, bootstrap_servers=['localhost:9092'],
                                 auto_offset_reset='earliest')
        consumer.subscribe([TOPIC])
        for kafka_msg in consumer:
            print(f'consumer id:{consumer_id} ', kafka_msg)
        consumer.close()
    except Exception:
        traceback.print_exc()
    pass


def begin_produce():
    print("begin produce")
    with ThreadPoolExecutor(max_workers=producer_num) as exe:
        futures = []
        for i in range(producer_num):
            future = exe.submit(thread_producer)
            futures.append(future)
    print("finish produce")
    pass


def begin_consume():
    print("begin consume")
    try:
        consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'])
        consumer_num = len(consumer.partitions_for_topic(TOPIC))
        with ThreadPoolExecutor(max_workers=consumer_num) as exe:
            futures = []
            for i in range(consumer_num):
                future = exe.submit(thread_consumer, i)
                futures.append(future)
            for future in as_completed(futures):
                pass
        consumer.close()
    except Exception:
        traceback.print_exc()
    print("finish consume")
    pass


if __name__ == '__main__':
    with ThreadPoolExecutor(max_workers=2) as exe:
        exe.submit(begin_produce)
        exe.submit(begin_consume)
    print("bye!")
    pass
