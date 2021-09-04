# Evaluate Human Balance with Spark Streaming

Jun Zhu
___

A small startup STEDI focuses on assessing balance of seniors. STEDI has 
an application that collects data from seniors during a small exercise. 
In this application, the user starts a timer and clicks a button with each step 
he/she takes. The test is finished when 30 steps are reached. The data 
collected enables the application to monitor the balance risk of the user. 

![Project Architecture](./architecture.jpg)

Redis is configured as a Kafka source. Whenever any data is saved to Redis, 
the message will be published to the Kafka topic called `redis-server`. After a 
customer has completed 4 or more assessments, the risk score will be 
transmitted to a Kafka topic called `stedi-events`. 

## Start the Spark ecosystem with Kafka and Redis

```
docker-compose up
```

### Check Kafka status

List the existing Kafka topics by

```sh
docker exec broker kafka-topics --bootstrap-server localhost:9092 --list
```

If all the services started correctly, you should be able to see the following
list of topics:
```
__confluent.support.metrics
__consumer_offsets
redis-server
stedi-events
```

### Check Redis status

Start the Redis CLI tool by

```
docker exec -it redis redis-cli
```

List the existing keys by
```sh
127.0.0.1:6379> keys **
```

The following keys are already in Redis:
```sh
1) "Customer"
2) "User"
```

## How to use

- Log in to the STEDI application: http://localhost:4567.

### Automatic data generation

- From the timer page, use the toggle button in the upper right corner to 
  activate simulated user data to see real-time customer and risk score data. 
  Toggle off and on to create additional customers for redis events. Each 
  time you activate it, STEDI creates 30 new customers, and then starts 
  generating risk scores for each one. It takes 4 minutes for each customer to 
  have risk scores generated, however customer data is generated immediately.

### Manual data generation (to be verified)

- Click Create New Customer, create a test customer and submit

- Click start, then add steps until you reach 30 and the timer has stopped

- Repeat this three times, and you will receive a risk score


## Analyse data

### Monitor data

Monitor the progress of data generation in STEDI by

```
docker logs -f stedi
```

The types of both "Customer" and "User" in Redis are `zset`. One can list the 
data by
```
zrange Customer 0 -1
```

Perform EDA on the two Kafka topics separately by

```
bash submit-event-kafka-stream.sh
bash submit-redis-kafka-stream.sh
```

### Run data processing pipeline

```
bash submit-kafka-join.sh
```

Once the data is populated in the configured kafka topic, the graph should have 
real-time data points.
