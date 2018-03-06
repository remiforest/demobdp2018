#!/usr/bin/python3

import json
import os
import time
import sys
import maprdb
import logging
from mapr_streams_python import Consumer, KafkaError

streams_path = '/mapr/global.mapr.com/streams/'

RAW_TABLE_PATH = "/mapr/global.mapr.com/tables/raw"
GKM_TABLE_PATH = "/mapr/global.mapr.com/tables/cargkm"
COUNT_TABLE_PATH = "/mapr/global.mapr.com/tables/count"

logging.basicConfig(filename='logs/centralprocess.log',level=logging.DEBUG)

consumers = {}

def open_db():
    return (maprdb.connect())

# create or get existing table
def open_table(connection, table_path):
    if connection.exists(table_path):
        return (connection.get(table_path))
    return (connection.create(table_path))


def get_streams(streams_path):
  streams = []
  for f in os.listdir(streams_path):
    if os.path.islink(streams_path + f):
      streams.append(streams_path + f)
  return streams

db = open_db()
t = open_table(db, RAW_TABLE_PATH)
gkm_table = open_table(db,GKM_TABLE_PATH)
count_table = open_table(db,COUNT_TABLE_PATH)

# Init count table
try:
  last_count = count_table.find_by_id("total_count")["count"]
except:
  last_count = 0
count_doc = {"_id":"total_count","count":last_count}
count_table.insert_or_replace(maprdb.Document(count_doc))
count_table.flush()


def update_consumers():
  global consumers
  streams = get_streams(streams_path)
  consumers_to_remove = []
  for consumer in consumers:
    if consumer not in streams:
      consumers_to_remove.append(consumer)
  for consumer_to_remove in consumers_to_remove:
    consumers[consumer_to_remove].close()
    del consumers[consumer_to_remove]

  # adding new consumers
  for stream in streams:
    if not stream in consumers:
      consumers[stream] = Consumer({'group.id': "global_process_group",'default.topic.config': {'auto.offset.reset': 'latest'}})
      consumers[stream].subscribe([stream+":default_topic"])
      logging.debug("subscribed to {}:{}".format(stream,"default_topic"))




while True:
    update_consumers()
    loop_count = 0
    buffer_count = 0
    for stream, consumer in consumers.items():
        running = True
        logging.debug("polling {}".format(stream))
        stream_count = 0
        while running:
          msg = consumer.poll(timeout=1.0)
          if msg is None:
            running = False
          else:
            if not msg.error():
              document = json.loads(msg.value().decode("utf-8"))
              model = document["model"]
              gkm = gkm_table.find_by_id(model)["gkm"]
              document["gkm"] = gkm
              document["_id"] = str(time.time())
              newdoc = maprdb.Document(document)
              t.insert_or_replace(newdoc)
              stream_count += 1
              buffer_count += 1
              if buffer_count % 100 == 0:
                try:
                  last_count = count_table.find_by_id("total_count")["count"]
                except:
                  last_count = 0
                new_count = last_count + buffer_count
                count_doc = {"_id":"total_count","count":new_count}
                count_table.insert_or_replace(maprdb.Document(count_doc))
                count_table.flush()
                # logging.debug("new count : {}".format(new_count))
                buffer_count = 0

        logging.debug("pulled {} events from {}".format(stream_count,stream))
        loop_count += stream_count

        try:
          last_count = count_table.find_by_id("total_count")["count"]
        except:
          last_count = 0
        new_count = last_count + buffer_count
        count_doc = {"_id":"total_count","count":new_count}
        count_table.insert_or_replace(maprdb.Document(count_doc))
        count_table.flush()

    t.flush()
    # time.sleep(1)

    logging.debug("Events pulled dring last iteration : {}".format(loop_count))

    
