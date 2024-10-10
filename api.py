from kafka import KafkaConsumer
import json
from connect import connect_to_mongo
from config import (
    kafka_pub_ip,
    kafka_pub_port,
    client_request_topic,
    client_response_topic,
    kafka_group_id,
)

from producer_utils import producer

db = connect_to_mongo()


def buy_fruit(db, fruit: str) -> bool:
    col = db["sales"]
    col.update_one({"name": fruit}, {"$inc": {"quantity": 1}}, upsert=True)
    return True


def get_sales(db) -> list:
    data = []
    col = db["sales"]
    for x in col.find():
        data.append(f"{x['name']} = {x['quantity']}")
    return data


def run_api_consumer():
    consumer = KafkaConsumer(
        client_request_topic,
        bootstrap_servers=[f"{kafka_pub_ip}:{kafka_pub_port}"],
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id=kafka_group_id,
        auto_offset_reset="latest",
    )
    for message in consumer:
        print(f"request Received message: {message.value}")
        id = message.value.get("request_id")
        print(id)
        if message.value["kind"] == "buy":
            buy_fruit(db, message.value["fruit"])
            producer.send(
                client_response_topic,
                {"kind": "buy", "status": "success", "request_id": id},
            )
        elif message.value["kind"] == "list":
            sales_list = get_sales(db)
            producer.send(
                client_response_topic,
                {"kind": "list", "list": sales_list, "request_id": id},
            )
