from kafka import KafkaProducer, KafkaConsumer
from crawl_5_media.localsettings import *

def get_kafka_producer():
    real_server_host = KAFKA_HOSTS
    if real_server_host:
        producer = KafkaProducer(bootstrap_servers=real_server_host)
        return producer
    else:
        return None