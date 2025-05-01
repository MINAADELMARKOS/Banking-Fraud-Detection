import logging
from config import Config  # Assuming you have a Config class like Scala's application.conf

class KafkaConfig:
    logger = logging.getLogger(__name__)
    kafka_params = {}

    @classmethod
    def load(cls):
        """Load Kafka settings from config"""
        cls.logger.info("Loading Kafka Settings")
        cls.kafka_params["topic"] = Config.application_conf.get("config.kafka.topic")
        cls.kafka_params["enable.auto.commit"] = Config.application_conf.get("config.kafka.enable.auto.commit")
        cls.kafka_params["group.id"] = Config.application_conf.get("config.kafka.group.id")
        cls.kafka_params["bootstrap.servers"] = Config.application_conf.get("config.kafka.bootstrap.servers")
        cls.kafka_params["auto.offset.reset"] = Config.application_conf.get("config.kafka.auto.offset.reset")

    @classmethod
    def default_setting(cls):
        """Use default settings when not running in a cluster"""
        cls.kafka_params["topic"] = "creditcardTransaction"
        cls.kafka_params["enable.auto.commit"] = "false"
        cls.kafka_params["group.id"] = "RealTime Creditcard FraudDetection"
        cls.kafka_params["bootstrap.servers"] = "localhost:9092"
        cls.kafka_params["auto.offset.reset"] = "earliest"

