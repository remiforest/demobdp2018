#! /usr/bin/python3

import sys
import random
import time
import json
import os
import argparse
from mapr_streams_python import Producer

parser = argparse.ArgumentParser(description='Launch a car stream producer')
parser.add_argument('--country',help='collector country')
parser.add_argument('--city',help='collector city')
parser.add_argument('--reset',help='delete historical data',action="store_true")
args = parser.parse_args()

if not args.country or not args.city:
    print("--country and --city required")
    sys.exit()


# Retrieves current cluster name


with open('/opt/mapr/conf/mapr-clusters.conf', 'r') as f:
    first_line = f.readline()
    cluster_name = first_line.split(' ')[0]
print(cluster_name)
sys.exit()

stream_path = '/mapr/global.mapr.com/countries/' + args.country  +'/streams/'

# if not os.path.isdir(stream_path):
#     os.system("mkdir -p " + stream_path)

stream_name = args.city
stream = stream_path + stream_name

# if not os.path.islink(stream):
#     print("creating stream {}".format(stream))
#     os.system('maprcli stream create -path ' + stream + ' -produceperm p -consumeperm p -topicperm p -copyperm p -adminperm p')
#     print("stream created")

# if args.reset :
#     print("deleting stream {}".format(stream))
#     os.system("maprcli stream delete -path " + stream)
#     print("recreating stream {}".format(stream))
#     os.system("maprcli stream create -path " + stream + " -produceperm p -consumeperm p -topicperm p -copyperm p -adminperm p")
#     print("stream created")




print("creating producer for {}".format(stream))
p = Producer({'streams.producer.default.stream': stream})


# Load cars from json file
carfile = "/mapr/global.mapr.com/data/carbycity.json"
cartable = "/mapr/global.mapr.com/data/carbycity"


# connection = connect()
# table = connection.get(cartable)
# print(table.find_by_id("Paris"))


with open(carfile) as f:
    for line in f:
        while True:
            try:
                city_json = json.loads(line)
                break
            except ValueError:
                # Not yet a complete JSON value
                line += next(f)
        models = city_json["cars"]
        traffic = float(city_json["traffic"])
        if city_json["_id"] == args.city:
            break

total_model_weight = 0
for model in models:
    total_model_weight += model["share"]

# print(total_model_weight)


colors = []
colors.append(("black",22))
colors.append(("white",20))
colors.append(("grey",18))
colors.append(("blue",10))
colors.append(("red",6))
colors.append(("brown",3))
colors.append(("green",1))

total_color_weight = 0
for c in colors:
    total_color_weight += c[1]


model_distrib = []
for model in models:
    weight = model["share"]
    for i in range(weight):
        model_distrib.append(model["car"])

color_distrib = []
for color in colors:
    weight = color[1]
    for i in range(weight):
        color_distrib.append(color[0])


print("Injecting ...")
nb_cars = 0
while True:
    # if not os.path.exists(stream):
    #     print("recreating stream {}".format(stream))
    #     os.system('maprcli stream create -path ' + stream + ' -produceperm p -consumeperm p -topicperm p -copyperm p -adminperm p')
    #     p = Producer({'streams.producer.default.stream':stream})
    car_model = model_distrib[random.randint(0,total_model_weight-1)]
    car_color = color_distrib[random.randint(0,total_color_weight-1)]
    message = {"timestamp":int(time.time()),"country":args.country,"city":args.city,"color":car_color,"model":car_model}
    # print(message)
    p.produce("default_topic", json.dumps(message))
    time.sleep(1/traffic)
    # p.flush()
    nb_cars += 1
    if nb_cars % 10 == 0:
        print("{} cars injected".format(nb_cars))


