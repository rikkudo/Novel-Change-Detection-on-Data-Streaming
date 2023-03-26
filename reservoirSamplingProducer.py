#!/usr/bin/env python

"""Generates a stream to Kafka from a time series csv file.
"""

import argparse
import csv
import json
import sys
import time
from dateutil.parser import parse
from confluent_kafka import Producer
import socket
import pandas as pd
import random

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg.value()), str(err)))
    else:
        print("Message produced: %s" % (str(msg.value())))

def reservoir_sampling(sampled_num, total_num):
    pool = []
    for i in range(0, total_num):
        if i < sampled_num:
            pool.append(i)
        else:
            r = random.randint(0, i)
            if r < sampled_num:
                pool[r] = i
    return pool



def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('filename', type=str,
                        help='Time series csv file.')
    parser.add_argument('Acceptedtopic', type=str,
                        help='Name of the Kafka topic to stream accepted lines.')
    parser.add_argument('Refusedtopic', type=str,
                        help='Name of the Kafka topic to stream refused lines.')
    parser.add_argument('sampled_num', type=float, default=1,
                        help='Size of the sample')
    parser.add_argument('sleepTimer', type=float, default=1,
                        help='Speed up time series by a given multiplicative factor.')
						
    args = parser.parse_args()

    Acceptedtopic = args.Acceptedtopic
    Refusedtopic = args.Refusedtopic
    p_key = args.filename

    conf = {'bootstrap.servers': "localhost:9092",
            'client.id': socket.gethostname()}
    producer = Producer(conf)
    
    rdr = pd.read_excel(args.filename, 'cdn_customer_qoe_anon')
    rdr.to_csv (r''+args.filename+'.csv', index = None, header=True)

	
    with open(args.filename+'.csv', 'r') as in_file:
        num_lines = sum(1 for line in in_file)
    print("NUM LINES: %d " % (num_lines))
    rdr = csv.reader(open(args.filename+'.csv'))
    header = next(rdr)

    if num_lines <= args.sampled_num:
        try:
            line = next(rdr, None)
            time.sleep(args.sleepTimer)
            res = dict(zip(header, line))
            timestamp, value = line[1], res
            result = {}
            result[timestamp] = value
            jresult = json.dumps(result)
            producer.produce(Acceptedtopic, key=p_key, value=jresult, callback=acked)
            producer.flush()
        except TypeError:
            sys.exit()
    else:
        sampled_counter = 0
        line_counter = 0
        sampled_indices = reservoir_sampling(args.sampled_num, num_lines)
        sampled_indices = set(sampled_indices)
        print("sampled_indices : "+str(sampled_indices))
        for i in range(0, num_lines):
            if sampled_counter >= args.sampled_num:
                break
            try:
                line = next(rdr, None)
                time.sleep(args.sleepTimer)
                res = dict(zip(header, line))
                timestamp, value = line[1], res
                result = {}
                result[timestamp] = value
                jresult = json.dumps(result)
                if line_counter in sampled_indices:
                    producer.produce(Acceptedtopic, key=p_key, value=jresult, callback=acked)
                else:
                    producer.produce(Refusedtopic, key=p_key, value=jresult, callback=acked)
                    sampled_counter += 1
                producer.flush()

            except TypeError:
                sys.exit()
                sampled_counter += 1
            line_counter += 1	

if __name__ == "__main__":
    main()
