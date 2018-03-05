#!/bin/bash

echo "Killing running processes"
pkill -f carwatch
pkill -f localfront
pkill -f globalfront

echo "Removing data tables"
rm -f /mapr/global.mapr.com/tables/raw
rm -f /mapr/global.mapr.com/tables/count
rm -f /mapr/global.mapr.com/tables/countries

echo "Removing source streams"
rm -rf /mapr/global.mapr.com/countries/*

echo "Removing target streams"
rm -rf /mapr/global.mapr.com/streams/*

echo "Recreating raw table"
maprcli table create -path /tables/raw -tabletype json
maprcli table edit -path /tables/raw -defaultreadperm p -defaultwriteperm p -defaulttraverseperm p -defaultmemoryperm p
maprcli table cf edit -path /tables/raw -cfname default -readperm p -writeperm p -traverseperm p

echo "Recreating count table"
maprcli table create -path /tables/count -tabletype json
maprcli table edit -path /tables/count -defaultreadperm p -defaultwriteperm p -defaulttraverseperm p -defaultmemoryperm p
maprcli table cf edit -path /tables/count -cfname default -readperm p -writeperm p -traverseperm p

echo "Recreating countries table"
maprcli table create -path /tables/countries -tabletype json
maprcli table edit -path /tables/countries -defaultreadperm p -defaultwriteperm p -defaulttraverseperm p -defaultmemoryperm p
maprcli table cf edit -path /tables/countries -cfname default -readperm p -writeperm p -traverseperm p

echo "Reset done."