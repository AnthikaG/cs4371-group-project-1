"""Shared runtime settings read from environment variables (and an optional .env file)."""

import os
from dataclasses import dataclass

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_INPUT_TOPIC = "topic1"
DEFAULT_ENTITY_TOPIC = "topic2"
DEFAULT_CLASS_TOPIC = "topic3"


@dataclass(frozen=True)
class Settings:
    """Kafka connection and topic names shared by every component."""

    kafka_bootstrap_servers: str
    kafka_input_topic: str
    kafka_entity_topic: str
    kafka_class_topic: str


def get_settings() -> Settings:
    """Build settings from the environment, falling back to the contract defaults."""
    return Settings(
        kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS),
        kafka_input_topic=os.getenv("KAFKA_INPUT_TOPIC", DEFAULT_INPUT_TOPIC),
        kafka_entity_topic=os.getenv("KAFKA_ENTITY_TOPIC", DEFAULT_ENTITY_TOPIC),
        kafka_class_topic=os.getenv("KAFKA_CLASS_TOPIC", DEFAULT_CLASS_TOPIC),
    )
