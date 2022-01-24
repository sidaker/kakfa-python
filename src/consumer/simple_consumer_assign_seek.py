import asyncio

from confluent_kafka import Consumer, OFFSET_BEGINNING, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

BROKER_URL = "PLAINTEXT://localhost:9092"
TOPIC_NAME = "com.topic.my.first.topic"

'''
https://antoniodimariano.medium.com/how-a-kafka-consumer-can-start-reading-messages-from-a-different-offset-and-get-back-to-the-start-eee28dc19428

https://antoniodimariano.medium.com/how-a-kafka-consumer-can-start-reading-messages-from-a-different-offset-and-get-back-to-the-start-eee28dc19428

Assign and seek are used to replay data or fetch a specific message.
Use assign to assign a specific Topic-partition
and seek to read from a specific offset.

Now let us start from offset x and read 10 messages and exit.
https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#topicpartition

'''

async def consume(topic_name):
    # To allows consumers in a group to resume at the right offset, I need to set group.id.

    c = Consumer(
        {
            "bootstrap.servers": BROKER_URL,
            "group.id": "3",
            "auto.offset.reset": "earliest",
        }
    )

    #c.subscribe([topic_name], on_assign=on_assign)
    #offsettoReadFrom=OFFSET_BEGINNING
    offsettoReadFrom=7147283
    #https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#topicpartition
    partitionsToReadFrom = TopicPartition(topic_name, 0, offsettoReadFrom)
    c.assign([partitionsToReadFrom])

    c.seek(partitionsToReadFrom)

    keepreading = True
    messagesread = 0
    while keepreading:
        message = c.poll(1.0)
        if message is None:
            print("no message received by consumer")
        elif message.error() is not None:
            print(f"error from consumer {message.error()}")
        else:
            print(f"consumed message with key {message.key()}: {message.value()}")
            print(f"Let us print Partition {message.partition()} and offset {message.offset()}")

        messagesread += 1
        if(messagesread >5):
            keepreading = False
            break
        await asyncio.sleep(0.1)


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
