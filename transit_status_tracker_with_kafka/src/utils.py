from confluent_kafka.admin import AdminClient, NewTopic

from .config import config

client = AdminClient({"bootstrap.servers": config["KAFKA"]["BROKER_URL"]})


def topic_exists(topic):
    """Checks if the given topic already exists in Kafka."""
    topics = [v.topic for v in client.list_topics(timeout=5).topics.values()]
    return topic in topics


def create_topic(name, *, num_partitions=1, num_replicas=1):
    """Create a Kafka topic."""
    # TODO: maybe add config for NewTopic
    futures = client.create_topics([
        NewTopic(topic=name,
                 num_partitions=num_partitions,
                 replication_factor=num_replicas)
    ])
    for _, future in futures.items():
        future.result()
