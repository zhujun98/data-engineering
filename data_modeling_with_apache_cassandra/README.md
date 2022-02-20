# DEND - Data Modeling with Apache Cassandra

Jun Zhu
___

A startup called Sparkify wants to analyze the data they've been collecting 
on songs and user activity on their new music streaming app. The analysis 
team is particularly interested in understanding **what songs users are 
listening to**. Currently, there is no easy way to query the data to generate 
the results, since the data reside in a directory of CSV files on user 
activity on the app.

They'd like a data engineer to create an Apache Cassandra database which can 
create queries on song play data to answer the questions, and wish to bring 
you on the project. Your role is to create a database for this analysis. 
You'll be able to test your database by running queries given to you by the 
analytics team from Sparkify to create the results.

## Datasets

The project has only one dataset: **event_data**. The directory of CSV files is 
partitioned by date. Here are examples of filepaths to two files in the dataset:

```angular2html
event_data/2018-11-08-events.csv
event_data/2018-11-09-events.csv
```

## Installing dependencies

```sh
pip install pandas cassandra-driver
```

## Running Cassandra locally

```sh
# Creat a Docker network.
docker network create cassandra-cluster-network

# Pull Apache Cassandra image from DockerHub.
docker pull cassandra

# Start a Cassandra node.
docker run --name cassandra-node-1 --network cassandra-cluster-network -p 127.0.0.1:9042:9042 -d cassandra

# Launch an existing Cassandra container.
docker start <CONTAINER ID>
```

## Getting started

Please follow this [Jupyter notebook](./data_modeling_with_apache_cassandra.ipynb).

## More readings

- [Understanding Cassandra’s data model and Cassandra Query Language (CQL)](https://www.oreilly.com/library/view/cassandra-the-definitive/9781491933657/ch04.html)
- [Designing a Cassandra Data Model](https://shermandigital.com/blog/designing-a-cassandra-data-model/)
- [Cassandra data modeling: The primary key](https://www.datastax.com/blog/most-important-thing-know-cassandra-data-modeling-primary-key)
