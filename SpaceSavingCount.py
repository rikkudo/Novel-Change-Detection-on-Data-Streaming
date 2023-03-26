import argparse
import json
import sys
import time
import socket
from confluent_kafka import Consumer, KafkaError, KafkaException
import random
import mmh3
import heapq
import math

class SpaceSavingCounter:
    def __init__(self):
        self.n = 0
        self.counts = dict()
        self.countscap=0
        #dictionary max size
        self.k = 10

    def inc(self, x):
        # increment total elements seen
        self.n += 1

        # x found
        if x in self.counts:
            self.counts[x] =self.counts[x]+1


        # x not found
        else:
            if self.countscap < self.k:
                self.counts.update({x:1})
                self.countscap= self.countscap+1
            else:
                self.lowest = min(self.counts, key=self.counts.get)
                self.counts[x]=1
                del self.counts[self.lowest]


#
def getEndOfPlaybackStatus(msg):
    val = msg.value()
    dval = json.loads(val)
    status = dval['End of Playback Status']
    return status

def main():

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

    # sketch 3
    counter = SpaceSavingCounter()


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
                
                status = getEndOfPlaybackStatus(msg)
                counter.inc(status)
                print("update")
                print(counter.counts)




                

    except KeyboardInterrupt:
        pass

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


if __name__ == "__main__":
    main()
