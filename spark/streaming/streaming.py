from pyspark.sql.functions import current_date, datediff, to_date
from pyspark.sql.types import IntegerType
from pyspark.ml import PipelineModel
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition
import os
import time




customer_df = spark.read \
    .format("org.apache.spark.sql.HBase") \
    .options(table="customer", keyspace=HBaseConfig.keyspace) \
    .load()

customer_age_df = customer_df.withColumn(
    "age",
    (datediff(current_date(), to_date(customer_df["dob"])) / 365).cast(IntegerType())
).cache()




preprocessing_model = PipelineModel.load(SparkConfig.preprocessingModelPath)
random_forest_model = RandomForestClassificationModel.load(SparkConfig.modelPath)


def read_offset():
    return spark.read \
        .format("org.apache.spark.sql.HBase") \
        .options(table=HBaseConfig.kafkaOffsetTable, keyspace=HBaseConfig.keyspace) \
        .load() \
        .filter(f"topic = '{KafkaConfig.kafkaParams['topic']}'") \
        .collect()
        
stored_offsets = read_offset()




ssc = StreamingContext(spark.sparkContext, SparkConfig.batchInterval)

if not stored_offsets:
    kafka_stream = KafkaUtils.createDirectStream(
        ssc, [KafkaConfig.kafkaParams["topic"]],
        KafkaConfig.kafkaParams
    )
else:
    from_offsets = {
        TopicAndPartition(KafkaConfig.kafkaParams["topic"], row['partition']): row['offset']
        for row in stored_offsets
    }
    kafka_stream = KafkaUtils.createDirectStream(
        ssc, [KafkaConfig.kafkaParams["topic"]],
        KafkaConfig.kafkaParams,
        fromOffsets=from_offsets
    )


def process_rdd(rdd):
    if not rdd.isEmpty():
        df = spark.read.json(rdd.map(lambda x: x[1]))  # assuming JSON format
        processed_df = preprocessing_model.transform(df)
        prediction_df = random_forest_model.transform(processed_df) \
            .withColumnRenamed("prediction", "is_fraud")
        save_predictions(prediction_df)
        save_offset(rdd)

kafka_stream.foreachRDD(process_rdd)


from HBase.cluster import Cluster

cluster = Cluster()
session = cluster.connect(HBaseConfig.keyspace)

prep_fraud = session.prepare(CreditcardTransactionRepository.cqlTransactionPrepare(
    HBaseConfig.keyspace, fraudTable))
prep_nonfraud = session.prepare(CreditcardTransactionRepository.cqlTransactionPrepare(
    HBaseConfig.keyspace, nonFraudTable))

def save_predictions(df):
    for row in df.collect():
        if row['is_fraud'] == 1.0:
            session.execute(prep_fraud.bind(row.asDict()))
        elif row['is_fraud'] == 0.0:
            session.execute(prep_nonfraud.bind(row.asDict()))



prep_offset = session.prepare(
    KafkaOffsetRepository.cqlOffsetPrepare(HBaseConfig.keyspace, HBaseConfig.kafkaOffsetTable))

def save_offset(rdd):
    offset_map = {}
    for record in rdd.collect():
        partition = record[0].partition
        offset = record[0].offset
        offset_map[partition] = max(offset_map.get(partition, 0), offset)

    for partition, offset in offset_map.items():
        session.execute(prep_offset.bind([KafkaConfig.kafkaParams['topic'], partition, offset]))


def handle_graceful_shutdown(check_interval_millis, ssc, spark):
    while True:
        stopped = ssc.awaitTerminationOrTimeout(check_interval_millis / 1000)
        if stopped:
            print("Streaming context stopped. Exiting...")
            break
        if os.path.exists("/tmp/shutdownmarker"):
            print("Shutdown marker found. Stopping streaming context.")
            ssc.stop(stopSparkContext=True, stopGracefully=True)
            spark.stop()
            break
