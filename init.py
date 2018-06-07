#! /usr/bin/python

import os

# Retrieves current cluster name
with open('/opt/mapr/conf/mapr-clusters.conf', 'r') as f:
    first_line = f.readline()
    cluster_name = first_line.split(' ')[0]
    logging.debug('Cluster name : {}'.format(cluster_name))

command_line = "sudo rm -rf /mapr/" + cluster_name + "/tables"
os.system(command_line)

command_line = "sudo mkdir /mapr/" + cluster_name + "/tables"
os.system(command_line)

command_line = "sudo maprcli table create -path /tables/cargkm -tabletype json"
os.system(command_line)

command_line = "sudo maprcli table cf edit -path /tables/cargkm -cfname default readperm p -writeperm p -traverseperm p"
os.system(command_line)

command_line = "sudo mapr importJSON -src /mapr/" + cluster_name + "/demobdp2018/cargkm.json -dst /mapr/" + cluster_name + "/tables/cargkm -mapreduce false"
os.system(command_line)

command_line = "sudo maprcli table create -path /tables/count -tabletype json"
os.system(command_line)

command_line = "sudo rm -rf /mapr/" + cluster_name + "/streams"
os.system(command_line)

command_line = "sudo mkdir /mapr/" + cluster_name + "/streams"
os.system(command_line)

command_line = "sudo rm -rf /mapr/" + cluster_name + "/countries"
os.system(command_line)

command_line = "sudo mkdir /mapr/" + cluster_name + "/countries"
os.system(command_line)
