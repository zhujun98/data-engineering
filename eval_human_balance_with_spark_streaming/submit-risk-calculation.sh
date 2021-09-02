#!/bin/bash
docker exec -it spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 /home/workspace/risk_calculation.py | tee ./spark/logs/risk_calculation.log