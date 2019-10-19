#!/bin/bash

#JPID= ps ux | grep '/snap/intellij-idea-ultimate/173/jbr/bin/java -Xmx128M' | grep -v grep | awk '{print $2}\'
#echo $JPID

JPID=16932

#lover paranoidness of system
sh -c 'echo 1 >/proc/sys/kernel/perf_event_paranoid'

TIME=10

if [[ "$1" != "" ]]; then
    TIME="$1"
else
    TIME=20
fi

rm -rf output
mkdir output
chmod 777 output

# Разогрев
#wrk -s get.lua -c 4 -d 30 -t 4 -R 10k -L http://localhost:8080

aprof start -f 1_get_empty.svg 16932
wrk -s get.lua -c 4 -d $TIME -t 4 -R 10k -L http://localhost:8080 >> output/1_get_empty_wrk.txt
aprof stop 16932

aprof start 99 -f 2_put.svg 16932
wrk -s put.lua -c 4 -d $TIME -t 4 -R 10k -L http://localhost:8080 >> output/2_put_wrk.txt
aprof stop 16932

aprof start 99 -f 3_get_full.svg 16932
wrk -s get.lua -c 4 -d $TIME -t 4 -R 10k -L http://localhost:8080 >> output/3_get_full_wrk.txt
aprof stop 16932

aprof start 99 -f 4_delete.svg 16932
wrk -s delete.lua -c 4 -d $TIME -t 4 -R 10k -L http://localhost:8080 >> output/4_delete_wrk.txt
aprof stop 16932

#Hardcore tests

#wrk -s get.lua -c 4 -d $TIME -t 4 -R 1000k -L http://localhost:8080
#aprof start -f output/1_get_empty_million.svg jps
#wrk -s get.lua -c 4 -d $TIME -t 4 -R 1000k -L http://localhost:8080 >> output/1_get_empty_wrk_million.txt
#aprof stop jps
#
#wrk -s put.lua -c 4 -d $TIME -t 4 -R 1000k -L http://localhost:8080
#aprof start 99 -f output/2_put_million.svg jps
#wrk -s put.lua -c 4 -d $TIME -t 4 -R 1000k -L http://localhost:8080 >> output/2_put_wrk_million.txt
#aprof stop jps
#
#wrk -s get.lua -c 4 -d $TIME -t 4 -R 1000k -L http://localhost:8080
#aprof start 99 -f output/3_get_full_million.svg jps
#wrk -s get.lua -c 4 -d $TIME -t 4 -R 1000k -L http://localhost:8080 >> output/3_get_full_wrk_million.txt
#aprof stop jps
#
#wrk -s delete.lua -c 4 -d $TIME -t 4 -R 1000k -L http://localhost:8080
#aprof start 99 -f output/4_delete_million.svg jps
#wrk -s delete.lua -c 4 -d $TIME -t 4 -R 1000k -L http://localhost:8080 >> output/4_delete_wrk_million.txt
#aprof stop jps
