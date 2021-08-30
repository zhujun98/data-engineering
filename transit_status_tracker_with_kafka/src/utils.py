from confluent_kafka.admin import AdminClient

from .config import config

BROKER_URL = config["KAFKA"]["BROKER_URL"]


def topic_exists(topic):
    """Checks if the given topic already exists in Kafka."""
    client = AdminClient({"bootstrap.servers": BROKER_URL})
    topics = [v.topic for v in client.list_topics(timeout=5).topics.values()]
    return topic in topics
