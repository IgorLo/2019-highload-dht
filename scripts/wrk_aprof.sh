#!/bin/bash

#JPID= ps ux | grep '/snap/intellij-idea-ultimate/173/jbr/bin/java -Xmx128M' | grep -v grep | awk '{print $2}\'
#echo $JPID

TIME=10

if [[ "$1" != "" ]]; then
    TIME="$1"
else
    TIME=10
fi

rm -r output
mkdir output

wrk -s get.lua -c 4 -d $TIME -t 4 -R 10k -L http://localhost:8080
aprof start -f output/1_get_empty.svg jps
wrk -s get.lua -c 4 -d $TIME -t 4 -R 10k -L http://localhost:8080 >> output/1_get_empty_wrk.txt
aprof stop jps

wrk -s put.lua -c 4 -d $TIME -t 4 -R 10k -L http://localhost:8080
aprof start 99 -f output/2_put.svg jps
wrk -s put.lua -c 4 -d $TIME -t 4 -R 10k -L http://localhost:8080 >> output/2_put_wrk.txt
aprof stop jps

wrk -s get.lua -c 4 -d $TIME -t 4 -R 10k -L http://localhost:8080
aprof start 99 -f output/3_get_full.svg jps
wrk -s get.lua -c 4 -d $TIME -t 4 -R 10k -L http://localhost:8080 >> output/3_get_full_wrk.txt
aprof stop jps

wrk -s delete.lua -c 4 -d $TIME -t 4 -R 10k -L http://localhost:8080
aprof start 99 -f output/4_delete.svg jps
wrk -s delete.lua -c 4 -d $TIME -t 4 -R 10k -L http://localhost:8080 >> output/4_delete_wrk.txt
aprof stop jps

#Hardcore tests

wrk -s get.lua -c 4 -d $TIME -t 4 -R 1000k -L http://localhost:8080
aprof start -f output/1_get_empty.svg jps
wrk -s get.lua -c 4 -d $TIME -t 4 -R 1000k -L http://localhost:8080 >> output/1_get_empty_wrk.txt
aprof stop jps

wrk -s put.lua -c 4 -d $TIME -t 4 -R 1000k -L http://localhost:8080
aprof start 99 -f output/2_put.svg jps
wrk -s put.lua -c 4 -d $TIME -t 4 -R 1000k -L http://localhost:8080 >> output/2_put_wrk.txt
aprof stop jps

wrk -s get.lua -c 4 -d $TIME -t 4 -R 1000k -L http://localhost:8080
aprof start 99 -f output/3_get_full.svg jps
wrk -s get.lua -c 4 -d $TIME -t 4 -R 1000k -L http://localhost:8080 >> output/3_get_full_wrk.txt
aprof stop jps

wrk -s delete.lua -c 4 -d $TIME -t 4 -R 1000k -L http://localhost:8080
aprof start 99 -f output/4_delete.svg jps
wrk -s delete.lua -c 4 -d $TIME -t 4 -R 1000k -L http://localhost:8080 >> output/4_delete_wrk.txt
aprof stop jps
