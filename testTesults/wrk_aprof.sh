#!/bin/bash

#JPID=$(ps ux | grep Xmx128M | grep -v grep | awk '{print $2}\')
#echo $JPID
JPID=32082

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

aprof stop "$JPID"

# Разогрев
wrk -s get.lua -c 4 -d 20 -t 4 -R 1k -L http://localhost:8080

#------ CPU

aprof start -e cpu -t -f output/1_get_empty_cpu.svg "$JPID"
wrk -s get.lua -c 4 -d $TIME -t 4 -R 1k -L http://localhost:8080 >> output/1_get_empty_wrk.txt
aprof stop -e cpu -t -f output/1_get_empty_cpu.svg "$JPID"

aprof start -e cpu -t -f output/2_put_cpu.svg "$JPID"
wrk -s put.lua -c 4 -d $TIME -t 4 -R 1k -L http://localhost:8080 >> output/2_put_wrk.txt
aprof stop -e cpu -t -f output/2_put_cpu.svg "$JPID"

aprof start -e cpu -t -f output/3_get_full_cpu.svg "$JPID"
wrk -s get.lua -c 4 -d $TIME -t 4 -R 1k -L http://localhost:8080 >> output/3_get_full_wrk.txt
aprof stop -e cpu -t -f output/3_get_full_cpu.svg "$JPID"

aprof start -e cpu -t -f output/4_delete_cpu.svg "$JPID"
wrk -s delete.lua -c 4 -d $TIME -t 4 -R 1k -L http://localhost:8080 >> output/4_delete_wrk.txt
aprof stop -e cpu -t -f output/4_delete_cpu.svg "$JPID"

aprof start -e cpu -t -f output/5_range_cpu.svg "$JPID"
wrk -s range.lua -c 4 -d $TIME -t 4 -R 1k -L http://localhost:8080 >> output/5_range_wrk.txt
aprof stop -e cpu -t -f output/5_range_cpu.svg "$JPID"

#------ ALLOC

aprof start -e alloc -t -f output/1_get_empty_alloc.svg "$JPID"
wrk -s get.lua -c 4 -d $TIME -t 4 -R 1k -L http://localhost:8080 >> output/1_get_empty_wrk.txt
aprof stop -e alloc -t -e cpu -f output/1_get_empty_alloc.svg "$JPID"

aprof start -e alloc -t -e cpu  -f output/2_put_alloc.svg "$JPID"
wrk -s put.lua -c 4 -d $TIME -t 4 -R 1k -L http://localhost:8080 >> output/2_put_wrk.txt
aprof stop -e alloc -t -f output/2_put_alloc.svg "$JPID"

aprof start -e alloc -t -f output/3_get_full_alloc.svg "$JPID"
wrk -s get.lua -c 4 -d $TIME -t 4 -R 1k -L http://localhost:8080 >> output/3_get_full_wrk.txt
aprof stop -e alloc -t -f output/3_get_full_alloc.svg "$JPID"

aprof start -e alloc -t -f output/4_delete_alloc.svg "$JPID"
wrk -s delete.lua -c 4 -d $TIME -t 4 -R 1k -L http://localhost:8080 >> output/4_delete_wrk.txt
aprof stop -e alloc -t -f output/4_delete_alloc.svg "$JPID"

aprof start -e alloc -t -f output/5_range_alloc.svg "$JPID"
wrk -s range.lua -c 4 -d $TIME -t 4 -R 1k -L http://localhost:8080 >> output/5_range_wrk.txt
aprof stop -e alloc -t -f output/5_range_alloc.svg "$JPID"

#------ LOCK

aprof start -e lock -t -f output/1_get_empty_lock.svg "$JPID"
wrk -s get.lua -c 4 -d $TIME -t 4 -R 1k -L http://localhost:8080 >> output/1_get_empty_wrk.txt
aprof stop -e lock -t -f output/1_get_empty_lock.svg "$JPID"

aprof start -e lock -t -f output/2_put_lock.svg "$JPID"
wrk -s put.lua -c 4 -d $TIME -t 4 -R 1k -L http://localhost:8080 >> output/2_put_wrk.txt
aprof stop -e lock -t -f output/2_put_lock.svg "$JPID"

aprof start -e lock -t -f output/3_get_full_lock.svg "$JPID"
wrk -s get.lua -c 4 -d $TIME -t 4 -R 1k -L http://localhost:8080 >> output/3_get_full_wrk.txt
aprof stop -e lock -t -f output/3_get_full_lock.svg "$JPID"

aprof start -e lock -t -f output/4_delete_lock.svg "$JPID"
wrk -s delete.lua -c 4 -d $TIME -t 4 -R 1k -L http://localhost:8080 >> output/4_delete_wrk.txt
aprof stop -e lock -t -f output/4_delete_lock.svg "$JPID"

aprof start -e lock -t -f output/5_range_lock.svg "$JPID"
wrk -s range.lua -c 4 -d $TIME -t 4 -R 1k -L http://localhost:8080 >> output/5_range_wrk.txt
aprof stop -e lock -t -f output/5_range_lock.svg "$JPID"

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
