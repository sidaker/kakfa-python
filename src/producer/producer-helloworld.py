import json
import random

# pip install confluent-kafka
from confluent_kafka import Consumer, Producer

# python src/producer-helloworld.py

# https://docs.confluent.io/platform/current/tutorials/examples/clients/docs/python.html#client-examples-python
# https://github.com/confluentinc/examples/blob/7.0.1-post/clients/cloud/python/producer.py

BROKER_URL = "PLAINTEXT://localhost:9092"
TOPIC_NAME = "com.topic.my.first.topic"


def produce_sync(topic_name):
    """Produces data synchronously into the Kafka Topic"""
    p = Producer({"bootstrap.servers": BROKER_URL})

    for i in range(1,10):
        record_key = "siddu"
        record_value = json.dumps({'count': i})
        print(type(record_value)) ## String
        p.produce(topic_name, key=record_key, value=record_value)
        p.flush()



def main():
    """Checks for topic and creates the topic if it does not exist"""
    try:
        produce_sync(TOPIC_NAME)
    except KeyboardInterrupt as e:
        print("shutting down")


if __name__ == "__main__":
    main()
