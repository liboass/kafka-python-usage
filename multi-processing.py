# -*- encoding:utf-8 -*-

import time
import traceback
import logging
import math
import json
from multiprocessing import Pool, cpu_count
from concurrent.futures import ThreadPoolExecutor
from kafka import KafkaConsumer
from kafka import TopicPartition

TOPIC = "test"
BOOTSTRAP_SERVERS = ["localhost:9092"]
CONSUMER_GROUP = TOPIC
CPU_NUM = cpu_count() - 1


def thread_consumer(consumer_id):
    try:
        consumer = KafkaConsumer(group_id=CONSUMER_GROUP, bootstrap_servers=BOOTSTRAP_SERVERS,
                                 auto_offset_reset="earliest", enable_auto_commit=False)
        # consumer.assign([TopicPartition(TOPIC, consumer_id)])
        consumer.subscribe([TOPIC])
        for kafka_msg in consumer:
            msg_json = json.loads(kafka_msg.value)
            logging.info(f"consumer id:{consumer_id} msg:{msg_json['data']}")
            consumer.commit()
        consumer.close()
    except Exception:
        traceback.print_exc()


def process_consumer(process_id, partition_num):
    per_cpu_consumer_num = math.ceil(partition_num / CPU_NUM)
    begin_partition = process_id * per_cpu_consumer_num
    consumer_num = per_cpu_consumer_num
    if process_id == CPU_NUM - 1:
        consumer_num = partition_num % per_cpu_consumer_num
    with ThreadPoolExecutor(max_workers=consumer_num) as exe:
        for i in range(consumer_num):
            exe.submit(thread_consumer, begin_partition + i)


def begin_consume():
    logging.info("begin consume")
    try:
        consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP_SERVERS)
        partition_num = len(consumer.partitions_for_topic(TOPIC))
        pool = Pool()
        for i in range(CPU_NUM):
            pool.apply_async(process_consumer, args=(i, partition_num))
        pool.close()
        pool.join()
        consumer.close()
    except Exception:
        traceback.print_exc()
    logging.info("finish consume")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    begin_consume()
