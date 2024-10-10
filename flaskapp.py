"""
Main module that serve as web interface
"""

import threading
from flask import Flask, render_template, request

from kafka import KafkaConsumer
from config import (
    client_response_topic,
    kafka_group_id,
    kafka_pub_ip,
    kafka_pub_port,
    home_html,
    client_request_topic,
)
from api import run_api_consumer
from producer_utils import produce_client, producer
import json

app = Flask(__name__)
bg_color = "pink"


def run_client_consumer(request_id):
    consumer = KafkaConsumer(
        client_response_topic,
        bootstrap_servers=[f"{kafka_pub_ip}:{kafka_pub_port}"],
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id=kafka_group_id,
        consumer_timeout_ms=10000,
        auto_offset_reset="earliest",
    )
    for message in consumer:
        print(f"response Received message: {message.value}")
        id = message.value.get("request_id")
        print(id)
        if id != request_id:
            continue
        kind = message.value.get("kind")

        if kind == "buy":
            return ["Purchase successful"]
        elif kind == "list":
            return message.value.get("list", "No sales data available")
        else:
            print("Unknown message format received:", message.value)
            return ["Unknown response received"]


def mock_get_products(db):
    return ["banana", "apple", "mango"]


@app.route("/", methods=["GET", "POST"])
def home_page():
    """
    Main page
    """
    results = False
    prod_list = mock_get_products("a")
    # prod_list = get_products(db, producer)
    if request.method == "GET":
        return render_template(home_html, prod_list=prod_list, results=results)
    if request.method == "POST":
        kind = request.form["kind"]
        fruit = request.form["fruit"]
        request_id = produce_client(producer, client_request_topic, kind, fruit)
        results = run_client_consumer(request_id)
        return render_template(home_html, prod_list=prod_list, results=results)


if __name__ == "__main__":
    consumer_thread = threading.Thread(target=run_api_consumer)
    consumer_thread.daemon = True
    consumer_thread.start()
    app.run()
