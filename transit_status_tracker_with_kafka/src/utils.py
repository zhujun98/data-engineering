from confluent_kafka.admin import AdminClient

from .config import config

BROKER_URL = config["KAFKA"]["BROKER_URL"]


def normalize_station_name(name):
    return name.lower().replace(
        "/", "_and_").replace(
        " ", "_").replace(
        "-", "_").replace(
        "'", "")


def topic_exists(topic):
    """Checks if the given topic already exists in Kafka."""
    client = AdminClient({"bootstrap.servers": BROKER_URL})
    topics = [v.topic for v in client.list_topics(timeout=5).topics.values()]
    return topic in topics
