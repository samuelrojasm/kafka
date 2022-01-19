#confluent-kafka API
from confluent_kafka import Producer
from faker import Faker
import json
import time

fake = Faker()
conf = {'bootstrap.servers': "Broker01:9092"}
confluent_producer = Producer(conf)

def get_register_user():
    return {
        "name": fake.name(),
        "address": fake.address(),
        "created_at": fake.year()
    }

def json_serializer(data):
    return json.dumps(data).encode("utf-8")
    
def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}] @ offset {}'.format(msg.topic(), msg.partition(),msg.offset()))
        

if __name__ == '__main__':

    while 1 == 1:    
        data = get_register_user()
        data = json.dumps(data).encode("utf-8")
        record_key = "Repositorio-A"
        print("Producing record: {}\t{}".format(record_key, data))
        confluent_producer.produce("mi-topic", key=record_key, value=data,callback=delivery_report)
        time.sleep(4)
        # p.poll() serves delivery reports (on_delivery)
        # from previous produce() calls.
        confluent_producer.poll(0)