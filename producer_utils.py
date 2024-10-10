from kafka import KafkaProducer
import json
from config import kafka_pub_port, kafka_pub_ip
from random import randint

producer = KafkaProducer(
    bootstrap_servers=[f"{kafka_pub_ip}:{kafka_pub_port}"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def produce_client(producer, topic, kind, fruit):
    rand = randint(0, 1000)
    producer.send(
        topic,
        {"kind": kind, "fruit": fruit, "request_id": rand},
    )
    return rand
