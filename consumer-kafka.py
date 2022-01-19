#confluent-kafka API
from confluent_kafka import Consumer

conf = {'bootstrap.servers': "Broker01:9092",
        'group.id': 'mi-primera-app',
        'auto.offset.reset': 'earliest'}
c = Consumer(conf)

if __name__ == '__main__':
    c.subscribe(['mi-topic'])
    try:
        while True:
            msg = c.poll(1.0)

            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                # print("Waiting for message or event/error in poll()")
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue

            print('Received message: {} Key: {}'.format(msg.value().decode('utf-8'), msg.key().decode('utf-8')))

    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        c.close()