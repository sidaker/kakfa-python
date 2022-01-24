import asyncio

from confluent_kafka import Consumer, OFFSET_BEGINNING
from confluent_kafka.admin import AdminClient, NewTopic

BROKER_URL = "PLAINTEXT://localhost:9092"
TOPIC_NAME = "com.topic.my.first.topic"

async def consume(topic_name):

    c = Consumer(
        {
            "bootstrap.servers": BROKER_URL,
            "group.id": "0",
            "auto.offset.reset": "earliest",
        }
    )

    # TODO: Configure the on_assign callback
    c.subscribe([topic_name], on_assign=on_assign)

    while True:
        message = c.poll(1.0)
        if message is None:
            print("no message received by consumer")
        elif message.error() is not None:
            print(f"error from consumer {message.error()}")
        else:
            print(f"consumed message with key {message.key()}: {message.value()}")
        await asyncio.sleep(0.1)


def on_assign(consumer, partitions):
    """Callback for when topic assignment takes place"""
    # TODO: If the topic is configured to use `offset_earliest` set the partition offset to
    # the beginning or earliest
    for partition in partitions:
        partition.offset = OFFSET_BEGINNING
        wmarks = consumer.get_watermark_offsets(partition)
        print(partition, wmarks)

    # TODO: Assign the consumer the partitions
    consumer.assign(partitions)


def main():
    """Runs the exercise"""
    client = AdminClient({"bootstrap.servers": BROKER_URL})
    try:
        asyncio.run(produce_consume(TOPIC_NAME))
    except KeyboardInterrupt as e:
        print("shutting down")

async def produce_consume(topic_name):
    """Runs the Consumer task"""
    t2 = asyncio.create_task(consume(topic_name))
    await t2

if __name__ == "__main__":
    main()
