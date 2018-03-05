#!/bin/bash
rm -f /mapr/global.mapr.com/tables/cargkm
maprcli table create -path /tables/cargkm -tabletype json
maprcli table cf edit -path /tables/cargkm -cfname default readperm p -writeperm p -traverseperm p
mapr importJSON -src /mapr/global.mapr.com/source/demobdp2018/cargkm.json -dst /mapr/global.mapr.com/tables/cargkm -mapreduce false
rm -f /mapr/global.mapr.com/tables/count
maprcli table create -path /tables/count -tabletype json

