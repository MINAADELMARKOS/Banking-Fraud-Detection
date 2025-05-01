from pyspark.sql import SparkSession, DataFrame

class KafkaSource

    @staticmethod
    def get_kafka_source(spark SparkSession, kafka_params dict) - DataFrame
        return (spark.readStream
                .format(kafka)
                .option(kafka.bootstrap.servers, kafka_params[bootstrap.servers])
                .option(subscribe, kafka_params[topic])
                .option(startingOffsets, kafka_params[auto.offset.reset])
                .load())
