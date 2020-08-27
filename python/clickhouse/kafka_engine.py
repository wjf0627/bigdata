#!/usr/local/bin/python2
# encoding=utf-8

from google.protobuf.internal.encoder import _VarintBytes
import google.protobuf.timestamp_pb2
from kafka import KafkaProducer

import clickhouse.kafka_pb2 as kafka_pb


def kafka_produce_protobuf_messages(topic, start_index, num_messages):
    #   data = ''
    for i in range(start_index, num_messages):
        msg = kafka_pb.KeyValuePair()
        msg.key = i
        msg.value = str(i * 2)
        serialized_msg = msg.SerializeToString()
        data = _VarintBytes(len(serialized_msg)) + serialized_msg
        producer = KafkaProducer(bootstrap_servers="localhost:9092")
        producer.send(topic=topic, value=data)
        producer.flush()
        print("Produced {} messages for topic {}".format(data, topic))


def send_kafka_protobuf():
    kafka_produce_protobuf_messages('kafka_proto', 0, 200)
    # kafka_produce_protobuf_messages('pb', 1, 2)
    # kafka_produce_protobuf_messages('pb', 3, 4)


if __name__ == '__main__':
    send_kafka_protobuf()
