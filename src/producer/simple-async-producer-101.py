import asyncio
from confluent_kafka import Consumer, Producer


BROKER_URL = "PLAINTEXT://localhost:9092"
TOPIC_NAME = "com.topic.my.first.topic"


async def produce_data(topic_name):
    """Produces data asynchronously into the Kafka Topic"""
    p = Producer({"bootstrap.servers": BROKER_URL})
    curr_iter = 0

    while True:
        p.produce(topic_name, f"Message {curr_iter}")
        curr_iter += 1
        await asyncio.sleep(1)

'''
You get the below error with out below
RuntimeWarning: coroutine 'produce_data' was never awaited
  produce_data(TOPIC_NAME)
RuntimeWarning: Enable tracemalloc to get the object allocation traceback
'''

async def produce():
    t1 = asyncio.create_task(produce_data(TOPIC_NAME))
    await t1

def main():
    """Assumes topic exists and writes data to the topic"""
    try:
        asyncio.run(produce())
    except KeyboardInterrupt as e:
        print("shutting down")


if __name__ == "__main__":
    main()
