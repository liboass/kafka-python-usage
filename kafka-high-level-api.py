# -*- coding:utf-8 -*-

import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka import TopicPartition

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
        consumer = KafkaConsumer(group_id=TOPIC, bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest')
        # 获取所有主题名
        # print(consumer.topics())

        # 获取指定主题的分区
        # print(consumer.partitions_for_topic(TOPIC))

        # 为当前consume分配分区
        consumer.assign([TopicPartition(TOPIC, consumer_id)])

        # 获取当前consumer已分配的分区
        # print(consumer.assignment())

        # 重置当前consumer组在指定分区的偏移
        # consumer.seek(TopicPartition(TOPIC, 1), 0)
        # consumer.seek_to_beginning(TopicPartition(TOPIC, 1))

        # 获取当前consumer组在分区的最新消息的偏移
        # print(consumer.end_offsets(consumer.assignment()))

        # 获取当前consumer组在指定分区的下一条将要消费消息的偏移
        # print(consumer.position(TopicPartition(TOPIC, consumer_id)))
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
