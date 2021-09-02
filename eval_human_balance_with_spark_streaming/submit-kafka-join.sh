#!/bin/bash
docker exec -it spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 /home/workspace/kafka_join.py | tee ./spark/logs/kafka_join.log