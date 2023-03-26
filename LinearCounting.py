### Linear Counting: counting distinct countries ###

import random
from bitarray import bitarray
import math
import argparse
import json
import sys
import time
import socket
from confluent_kafka import Consumer, KafkaError, KafkaException
import random
import mmh3
class LinearCounting:
    def __init__(self):
        #D is upper bound on the number of distinct elements
        #p>0 is a load factor
        self.D = 16
        self.p = 1
        #initializing size of bitarray, and creating bitarray 
        self.size = int(self.D/self.p)
        self.B = bitarray(self.size)
        self.B.setall(0)


    def update(self,element):
        address= mmh3.hash(element) % self.size
        self.B[address] = 1

    def query(self):
        nr_empty= self.B.count(0)
        w = nr_empty/self.size
        z = math.log(1/w)
        count = int(self.size*z)
        print("{} Distinct Countries Detected from stream".format(count))

def getCountry(msg):
    val = msg.value()
    dval = json.loads(val)
    country = dval['Country_N']
    return country

def main():
    counter = LinearCounting()
    # stream parsinng code 
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('topic', type=str,
                        help='Name of the Kafka topic to stream.')

    args = parser.parse_args()

    conf = {'bootstrap.servers': 'localhost: 29094',
            'default.topic.config': {'auto.offset.reset': 'smallest'},
            'group.id': socket.gethostname()}

    consumer = Consumer(conf)

    running = True


    try:
        while running:
            consumer.subscribe([args.topic])

            msg = consumer.poll(1)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    sys.stderr.write('Topic unknown, creating %s topic\n' %
                                     (args.topic))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                country = getCountry(msg)
                print("streaming detected from country :{}".format(country))
                counter.update(str(country))
                counter.query()

                

    except KeyboardInterrupt:
        pass

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


if __name__ == "__main__":
    main()



