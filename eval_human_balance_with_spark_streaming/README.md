# Evaluate Human Balance with Spark Streaming

Jun Zhu
___

You work for the data science team at STEDI, a small startup focused on 
assessing balance for seniors. STEDI has an application that collects data 
from seniors during a small exercise. The user logs in, and then selects the 
customer they are working with. Then the user starts a timer, and clicks a 
button with each step the senior takes. When the senior has reached 30 steps, 
their test is finished. The data transmitted enables the application to monitor 
seniors’ balance risk. 

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

## Generate data

- Log in to the STEDI application: http://localhost:4567

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


## Monitor data

Monitor the progress of data generation in STEDI by

```
docker logs -f stedi
```

The types of both "Customer" and "User" in Redis are `zset`. One can list the 
data by
```
zrange Customer 0 -1
```

## Analyse data

The STEDI data science team has configured some real-time data sources using 
Kafka Connect. One of those data sources is Redis. When a customer is first 
assessed in the STEDI application, their record is added to a sorted set 
called Customer in redis. Redis is running in a docker container on the default 
redis port (6379). There is no redis password configured. Redis is configured 
as a Kafka source, and whenever any data is saved to Redis (including Customer 
information), a payload is published to the Kafka topic called redis-server.

The application development team has programmed certain business events to be 
published automatically to Kafka. Whenever a customer takes an assessment, 
their risk score is generated, as long as they have four or more completed 
assessments. The risk score is transmitted to a Kafka topic called 
`stedi-events`. The `stedi-events` Kafka topic has a String key and a String 
value as a JSON object with this format:

```json
{
  "customer":"Jason.Mitra@test.com",
  "score":7.0,
  "riskDate":"2020-09-14T07:54:06.417Z"
}
```

There are three Spark Python script:

- Write one spark script [](redis_kafka_stream.py) to subscribe to the 
  `redis-server` topic, base64 decode the payload, and deserialize the JSON 
  to individual fields, then print the fields to the console. The data should 
  include the birth date and email address. You will need these.
- Write a second spark script [](event_kafka_stream.py) to subscribe to the 
  `stedi-events` topic and deserialize the JSON (it is not base64 encoded) to 
  individual fields. You will need the email address and the risk score.
- Write a spark script [](kafka_join.py) to join the customer dataframe and 
  the customer risk dataframes, joining on the email address. Create a JSON 
  output to the newly created kafka topic you configured for STEDI to subscribe 
  to that contains at least the fields below:

Once the data is populated in the configured kafka topic, the graph should have real-time data points
