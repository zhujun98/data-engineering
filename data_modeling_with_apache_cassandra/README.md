# Data Modeling with Apache Cassandra


## Dependencies

```sh
pip install pandas cassandra-drive

```

## Running Cassandra locally

```sh
# Create a Docker network.
docker network create cassandra-cluster-network

# Pull Apache Cassandra image from DockerHub.
docker pull cassandra

# Start a Cassandra node
docker run -d --name cassandra-node-1 --network cassandra-cluster-network -p 127.0.0.1:9042:9042 cassandra
```

## Getting started

The Cassandra client drivers for different programming languages can be found
[here](https://cassandra.apache.org/doc/latest/getting_started/drivers.html).
