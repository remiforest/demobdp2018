#!/bin/bash
# Copyright (c) 2018 & onwards. REMI ET RAPHAEL YOLO

INSTALL_DIR="/opt/mapr"
MAPRCLI="${INSTALL_DIR}/bin/maprcli"
CLUSTERS=$(ls /mapr)

function print_help { echo "replicateStream.sh
-s <SOURCE_DIRECTORY>
-t <TARGET_DIRECTORY>

Example: replicateStream.sh -s /mapr/<SOURCE_CLUSTER>/apps/ -t /mapr/<TARGET_CLUSTER>/apps/

Available clusters:
$CLUSTERS
" >&2;}

while getopts "s:t:" name; do
    case $name in
        s) s=$OPTARG;;
        t) t=$OPTARG;;
        ?) print_help; exit;;
    esac
done

if [[ ${s} != *"/mapr/"* ]];then
    print_help
    exit
fi
if [[ ${t} != *"/mapr/"* ]];then
    print_help
    exit
fi
if [ ! -d "$s" ]; then
    echo "Source path '$s' does not exit"
fi
if [ ! -d "$t" ]; then
    echo "Target path '$t' does not exit"
    exit
fi


STREAM_TO_REPLICATE=(`ls ${s}`)

for i in "${STREAM_TO_REPLICATE[@]}"
do :
      if [ ! -L "$t$i" ]
      then :
        echo "Replicating $s$i into $t$i..."
        MAPRCLI_QUERY="$MAPRCLI stream replica autosetup -path $s$i -replica $t$i"
        echo $MAPRCLI_QUERY
        $MAPRCLI_QUERY
      fi
done
