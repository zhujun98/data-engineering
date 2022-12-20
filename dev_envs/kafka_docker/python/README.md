# Kafka Python API

### Setup and install requirements

```sh
conda create -n kafka --python=3.9.12
conda activate kafka

conda activate kafka
cd python
pip install -r requirements.txt
```

### Synchronous producer and consumer:

```sh
# terminal 1
python producer_consumer.py --sync
# terminal 2
python producer_consumer.py --produce 5000 --sync
```

### Asynchronous producer and consumer:

```sh
# starting both the producer and consumer
python producer_consumer.py --produce 5000
```

, or with Avro schema:

```sh
python producer_consumer_with_avro.py --produce 5000
```
