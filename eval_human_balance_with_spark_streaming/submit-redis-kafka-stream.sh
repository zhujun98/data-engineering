#!/bin/bash
docker exec -it spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 /home/workspace/redis_kafka_stream.py | tee ./spark/logs/redis_stream.log