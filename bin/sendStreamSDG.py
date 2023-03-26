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
from ctgan import CTGAN

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg.value()), str(err)))
    else:
        print("Message produced: %s" % (str(msg.value())))
        #err
        


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('filename', type=str,
                        help='Time series csv file.')
    parser.add_argument('topic', type=str,
                        help='Name of the Kafka topic to stream sythetic data.')
    parser.add_argument('--speed', type=float, default=1, required=False,
                        help='Speed up time series by a given multiplicative factor.')
    args = parser.parse_args()

    topic = args.topic
    p_key = args.filename

    conf = {'bootstrap.servers': "localhost:9092",
            'client.id': socket.gethostname()}
    producer = Producer(conf)
    
    ## read excel file of original data
    df_orig = pd.read_excel(args.filename, 'cdn_customer_qoe_anon')
    
    ## Create synthetic data
    df = df_orig.copy()

    # some cleaning
    df.drop(['Column1'], axis=1, inplace=True)
    df.drop(['Program_N'], axis=1, inplace=True)
    df.drop(['Playtime'], axis=1, inplace=True)
    df.drop(['CDN Node Host'], axis=1, inplace=True)
    df.drop(['Happiness Value'], axis=1, inplace=True)
    df.drop(['Start Time'], axis=1, inplace=True)
    df.drop(['End Time'], axis=1, inplace=True)
    df.drop(['Content_TV_Show_N'], axis=1, inplace=True)
    df.drop(['Device ID'], axis=1, inplace=True)
    df['Browser Version'].fillna(df['Browser Version'].mode()[0], inplace = True)
    df['Crash Status'] = df['Crash Status'].fillna('no crash')
    df['End of Playback Status'].fillna(df['End of Playback Status'].mode()[0], inplace = True)

    # Names of the columns that are discrete
    discrete_columns =  [
        'Connection Type', 
        'Device', 
        'Device Type', 
        'Browser', 
        'Browser Version', 
        'OS', 
        'OS Version', 
        'Crash Status', 
        'End of Playback Status', 
        'Country_N', 
        'Region_N'
        ]
    # fit the modal
    ctgan = CTGAN(epochs=6)
    ctgan.fit(df , discrete_columns)

    # syn. data
    df_syn = ctgan.sample(5000)
    df_syn.to_csv('data/syn_daya.csv')

    # read the csv file of original data
    rdr = csv.reader(open('data/syn_daya.csv'))

    

    header = next(rdr)  # Skip header
    firstline = True
    i = 0

    while True:
        
        try:
            
            line = next(rdr, None)
            
            diff = (1)/args.speed
            time.sleep(diff)

            # Convert csv columns to key value pair
            res = dict(zip(header, line))

            # Convert dict to json as message format
            jresult = json.dumps(res)

            i= i + 1
            p_key = str(i)

            producer.produce(topic, key=p_key, value=jresult, callback=acked)

            producer.flush()

        except TypeError:
            sys.exit()


if __name__ == "__main__":
    main()