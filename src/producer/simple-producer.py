from confluent_kafka import Consumer, Producer


BROKER_URL = "PLAINTEXT://localhost:9092"
TOPIC_NAME = "com.topic.my.first.topic"


def produce_data(topic_name):
    """Produces data asynchronously into the Kafka Topic"""
    p = Producer({"bootstrap.servers": BROKER_URL, "client.id": "testid01", })
    curr_iter = 0

    while True:
        p.produce(topic_name, f"Simple Message {curr_iter}")
        curr_iter += 1
        ##https://stackoverflow.com/questions/62408128/buffererror-local-queue-full-in-python
        ## BufferError: Local: Queue full. You'll get this unessl you set producer properties.
        p.poll(0)
        ##call poll() at regular intervals to serve the producer's delivery report callbacks.
        ## Alternatively try if a sleep helps.


def main():
    """Assumes topic exists and writes data to the topic"""
    try:
        produce_data(TOPIC_NAME)
    except KeyboardInterrupt as e:
        print("shutting down")



if __name__ == "__main__":
    main()
