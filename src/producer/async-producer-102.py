from dataclasses import dataclass, field
import json
import random
import socket

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from faker import Faker


faker = Faker()

BROKER_URL = "PLAINTEXT://localhost:9092"
TOPIC_NAME = "com.topic.my.first.topic"


@dataclass
class Purchase:
    username: str = field(default_factory=faker.user_name)
    currency: str = field(default_factory=faker.currency_code)
    amount: int = field(default_factory=lambda: random.randint(100, 2000))

    def serialize(self):
        """Serializes the object in JSON string format"""
        # TODO: Serializer the Purchase object
        #       See: https://docs.python.org/3/library/json.html#json.dumps
        return json.dumps(
            {
                "username": self.username,
                "currency": self.currency,
                "amount": self.amount,
            }
        )

def acked(err, msg):
    '''
    Executes everytime a record has been successfully sent or an exception is thrown
    '''
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))


def produce_async(topic_name):
    """Produces data asynchronously into the Kafka Topic"""
    p = Producer({"bootstrap.servers": BROKER_URL, "batch.num.messages": 100,
    "queue.buffering.max.messages":2000,
    "client.id": socket.gethostname()
    })

    # TODO: Write a synchronous production loop.
    #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Producer.flush
    while True:
        # TODO: Instantiate a `Purchase` on every iteration. Make sure to serialize it before
        #       sending it to Kafka!
        p.produce(topic_name, Purchase().serialize(), callback=acked)
        ## https://docs.confluent.io/clients-confluent-kafka-python/current/overview.html
        ## p.flush() ## This is async, hence commented.
        ## But because there is no p.flush() or p.poll() it fails very soon for Queue Full.
        ## so we added -	batch.num.messages, -	queue.buffering.max.messages. Still we got failures.
        ## https://youtu.be/bipt30mO3so
        ## https://youtu.be/7857lLetsw4
        ## https://www.confluent.io/blog/kafka-python-asyncio-integration/
        # Wait up to 1 second for events. Callbacks will be invoked during
        ##this method call if the message is acknowledged.
        p.poll(1)
        ## https://docs.confluent.io/clients-confluent-kafka-python/current/overview.html


def main():
    """Checks for topic and creates the topic if it does not exist"""
    try:
        produce_async(TOPIC_NAME)
    except KeyboardInterrupt as e:
        print("shutting down")


if __name__ == "__main__":
    main()
