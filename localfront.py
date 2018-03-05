#! /usr/bin/python

import os
import json
import urllib
import logging
import argparse
import socket
import logging
import time
import random
from flask import Flask, render_template, request, jsonify
from mapr_streams_python import Consumer, KafkaError



parser = argparse.ArgumentParser(description='Launch the local UI for stream monitoring')
parser.add_argument('--port',help='webserver port',default="80")
parser.add_argument('--country',help='deployment country')
args = parser.parse_args()

consumers = {}

country = args.country
port = int(args.port)

logging.basicConfig(filename='localfront_'+country+'.log',level=logging.DEBUG)

logging.debug("Port : {}".format(port))

# Retrieves current cluster name
with open('/opt/mapr/conf/mapr-clusters.conf', 'r') as f:
    first_line = f.readline()
    cluster_name = first_line.split(' ')[0]
    logging.debug('Cluster name : {}'.format(cluster_name))

streams_path = '/mapr/'+ cluster_name + '/countries/' + country  +'/streams/'

# Create stream folder if not exists
if not os.path.isdir(streams_path):
    os.system("mkdir -p " + streams_path)




# MaprR streams related functions
def get_available_streams(streams_path): # returns a list with full path of all streams available in the stream path
  streams = []
  for f in os.listdir(streams_path):
    if os.path.islink(streams_path + f):
      streams.append(streams_path + f)
  return streams

def get_cities(streams_path): # returns a list of all the cities available (each city is a stream)
  cities = []
  for f in os.listdir(streams_path):
    if os.path.islink(streams_path + f):
      cities.append(f)
  return cities


def update_consumers(): # Updates the active consumers
  logging.debug("update consumers")
  global consumers
  streams = get_available_streams(streams_path)
  logging.debug("Current consumers :")
  logging.debug(consumers)
  logging.debug("Streams to consume :")
  logging.debug(streams)
  
  # clean consumers
  consumers_to_remove = []
  for stream,consumer in consumers.items():
    if stream not in streams:
      consumers_to_remove.append(stream)
  if len(consumers_to_remove):
    logging.debug("consumers to remove :")
    logging.debug(consumers_to_remove)
    for consumer_to_remove in consumers_to_remove:
      consumers[consumer_to_remove].close()
      del consumers[consumer_to_remove]

  # creating new consumers
  for stream in streams:
    if not stream in consumers:
      logging.debug("subscribing to {}:{}".format(stream,"default_topic"))
      group = str(time.time())
      consumers[stream] = Consumer({'group.id': group,'default.topic.config': {'auto.offset.reset': 'earliest'}})
      consumers[stream].subscribe([stream+":default_topic"])
      logging.debug("subscribed to {}:{}".format(stream,"default_topic"))
  logging.debug("Final consumers :")
  logging.debug(consumers)



app = Flask(__name__)



@app.route('/')
def home():
  return render_template('localfront.html',country=country)


######  AJAX functions  ######
@app.route('/get_stream_data',methods = ['POST'])
def get_stream_data():
  # Variable definition
  logging.debug("get stream data")
  global consumers
  cities = json.loads(request.form["cities"])
  count = request.form["count"] == 'true'
  consolidate = request.form["consolidate"] == 'true'

  logging.debug("variables :")
  logging.debug(cities)
  logging.debug(count)
  logging.debug(consolidate)

  update_consumers()
  raw_data = {}
  count_data = {}
  stream_data = {}

  # Poll new vehicles
  for stream, consumer in consumers.items():
    raw_data[stream] = {}
    running = True
    logging.debug("polling {}".format(stream))
    while running:
      msg = consumer.poll(timeout=1.0)
      if msg is None:
        running = False
      else:
        if not msg.error():
          document = json.loads(msg.value().decode("utf-8"))
          model = document["model"]
          if model in raw_data[stream]:
            raw_data[stream][model] += 1
          else:
            raw_data[stream][model] = 1
        elif msg.error().code() != KafkaError._PARTITION_EOF:
          print(msg.error())
          running = False
        else:
          running = False

  # format data
  if consolidate:
    count_data["Global"] = {}
    for city,city_data in raw_data.items():
      for k,v in city_data.items():
        if k in count_data["Global"]:
          count_data["Global"][k] += v
        else:
          count_data["Global"][k] = v
  else:
    for stream,data in raw_data.items():
      count_data[stream.split('/')[-1]] = data

  stream_data = count_data
  # logging.debug(stream_data)
  return json.dumps(stream_data)

@app.route('/get_all_streams')
def get_all_streams():
  return json.dumps(get_cities(streams_path))


@app.route('/deploy_new_city',methods=['POST'])
def launch_container():
  new_city = request.form['city']
  traffic = random.randint(10,100)
  command_line = "python3 /mapr/global.mapr.com/source/demobdp2018/carwatch.py --country " + country + " --city " + new_city + " --traffic " + str(traffic) + " &"
  os.system(command_line)
  return "New city deployed"


update_consumers()
app.run(debug=False,host='0.0.0.0',port=port)
for k,c in consumers.items():
  c.close()




