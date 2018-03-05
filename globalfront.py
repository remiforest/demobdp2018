#! /usr/bin/python

import os
import subprocess
import threading
import json
import time
import random
from flask import Flask, render_template, request
from mapr_streams_python import Consumer, KafkaError
import maprdb
import logging

logging.basicConfig(filename='globalfront.log',level=logging.DEBUG)


# Retrieves current cluster name
with open('/opt/mapr/conf/mapr-clusters.conf', 'r') as f:
    first_line = f.readline()
    cluster_name = first_line.split(' ')[0]
    logging.debug('Cluster name : {}'.format(cluster_name))


# Global variables
consumers = {}  # List of active stream consumers
streams_path = '/mapr/' + cluster_name + '/streams/'   # Streams directory
GKM_TABLE_PATH = '/mapr/' + cluster_name + '/tables/cargkm'  # Path for the table that stores GKM information
COUNT_TABLE_PATH = '/mapr/' + cluster_name + '/tables/count'  # Path for the table that stores GKM information
COUNTRIES_TABLE_PATH = '/mapr/' + cluster_name + '/tables/countries'  # Path for the table that stores GKM information


# MaprDB related functions
def open_db():
    logging.debug("opening db")
    return (maprdb.connect())

def open_table(connection, table_path):
    logging.debug("opening table")
    if connection.exists(table_path):
        logging.debug("table exists, returning existing table")
        return (connection.get(table_path))
    logging.debug("Table doesn't exists, creating new table")
    return (connection.create(table_path))

def getgkm(table,model_name):
  # logging.debug("querying {} for {}".format(table,model_name))
  return table.find_by_id(model_name)["gkm"]




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



######  Web pages  #####

@app.route('/')
def home():
  return render_template('globalfront.html')



######  AJAX functions  ######

@app.route('/get_stream_data',methods = ['POST'])
def get_stream_data(): # Returns all stream data since last poll
  logging.debug("get stream data")

  # Variables definition
  global consumers
  cities = json.loads(request.form["cities"])
  count = request.form["count"] == 'true'
  consolidate = request.form["consolidate"] == 'true'

  logging.debug("variables :")
  logging.debug("cities : {}".format(cities))
  logging.debug("count : {}".format(count))
  logging.debug("consolidate : {}".format(consolidate))

  # updating consumers to make sure we don't miss data
  update_consumers()

  # data results for each stage
  raw_data = {}
  count_data = {}
  stream_data = {}

  # Poll new vehicles from all the streams
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
  logging.debug("raw data :")
  logging.debug(raw_data)

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

  logging.debug("count data : ")
  logging.debug(count_data)


  # convert to gkm if required
  if not count:
    db = open_db()
    gkm_table = open_table(db, GKM_TABLE_PATH)
    for city,data in count_data.items():
      stream_data[city]={}
      for model_name,model_count in data.items():
        gkm = getgkm(gkm_table,model_name)
        if "gkm" in stream_data[city]:
          stream_data[city]["gkm"] += gkm * model_count
          stream_data[city]["count"] += model_count
        else:
          stream_data[city]["gkm"] = gkm * model_count
          stream_data[city]["count"] = model_count
  else:
    stream_data = count_data
  logging.debug("stream data :")
  logging.debug(stream_data)

  return json.dumps(stream_data)

@app.route('/get_all_streams',methods = ['POST','GET'])
def get_all_streams(): # Returns the list of all available stream names
  return json.dumps(get_cities(streams_path))


@app.route('/replicate_streams') 
def replicate_streams():  # Replicate all stream to the stream_path directory
  country_list = os.listdir("/mapr/" + cluster_name + "/countries/")
  logging.debug("Country list :")
  logging.debug(country_list)
  for country in country_list:
    logging.debug("replicating streams from {}".format(country))
    command_line = '/mapr/' + cluster_name + '/source/demobdp2018/replicateStream.sh -s /mapr/' + cluster_name + '/countries/' + country + '/streams/ -t ' + streams_path
    logging.debug("command line : ")
    logging.debug(command_line)
    os.system(command_line)
  return "Done"


@app.route('/get_events_count')
def get_events_count(): # Returns the number of events in the raw db
  db = open_db()
  count_table = open_table(db, COUNT_TABLE_PATH)
  try:
    event_count = count_table.find_by_id("total_count")["count"]
  except:
    event_count = 0
  return json.dumps({"count":event_count})

@app.route('/get_countries')
def get_countries(): # Returns the number of events in the raw db
  db = open_db()
  country_table = open_table(db, COUNTRIES_TABLE_PATH)
  countries = []
  for c in country_table.find():
    countries.append(c)
  logging.debug(countries)
  return json.dumps({"countries":countries})

@app.route('/deploy_new_country',methods=['POST'])
def deploy_country():
  new_country = request.form['country']
  db = open_db()
  country_table = open_table(db, COUNTRIES_TABLE_PATH)
  try:
    country_port = country_table.find_by_id(new_country)["port"]
    return "Country already deployed"
  except:
    logging.debug("deploying {}".format(new_country))
    port = 8000
    for c in country_table.find():
      port = max(port,c["port"])
    port += 1
    count_doc = {"_id":new_country,"port":port}
    command_line = "python3 /mapr/" + cluster_name + "/source/demobdp2018/localfront.py --country " + new_country + " --port " + str(port) + " &"
    os.system(command_line)
    country_table.insert_or_replace(maprdb.Document(count_doc))
    country_table.flush()
    return "New country deployed"

@app.route('/remove_country',methods=['POST'])
def remove_country():
  country = request.form['country']
  db = open_db()
  country_table = open_table(db, COUNTRIES_TABLE_PATH)
  country_table.delete(country)
  country_table.flush()
  command_line = "pkill -f " + country
  os.system(command_line)
  command_line = "rm -rf /mapr/" + cluster_name + "/countries/" + country
  os.system(command_line)
  return "{} killed".format(country)



update_consumers()
app.run(debug=False,host='0.0.0.0',port=80)
for k,c in consumers.items():
  c.close()

