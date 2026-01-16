import json
import os

import pandas as pd
from dbx_tools import clients
from kafka import KafkaConsumer, TopicPartition
from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout

BOOTSTRAP_SERVERS = "kafka.lfpconnect.io:443"
SECURITY_PROTOCOL = "SSL"
SOURCE_TOPIC = "source"

KAFKA_OPTIONS = {
    "kafka.bootstrap.servers": BOOTSTRAP_SERVERS,
    "kafka.security.protocol": SECURITY_PROTOCOL,
}

KAFKA_OPTIONS_READ = {
    **KAFKA_OPTIONS,
    "subscribe": SOURCE_TOPIC,
    "includeHeaders": "true",
    "startingOffsets": json.dumps({SOURCE_TOPIC: {0: 46958}}),
}


KAFKA_OPTIONS_WRITE = {**KAFKA_OPTIONS, "topic": "events"}


def build_metadata(df):
    # key parsed as json if possible, otherwise string
    key_value = F.coalesce(
        F.try_parse_json(F.col("key").cast("string")),
        F.col("key").cast("string"),
    )

    # headers as map<string,variant> at root
    header_keys = F.expr("transform(headers, h -> h.key)")

    header_values = F.expr("""
                transform(
                    headers,
                    h ->
                        coalesce(
                            try_parse_json(cast(h.value as string)),
                            cast(h.value as string)
                        )
                )
            """)

    header_map = F.map_from_arrays(header_keys, header_values)

    # kafka metadata under "kafka", excluding value, headers, key, timestamp
    kafka_metadata_cols = [
        c for c in df.columns if c not in ("value", "headers", "key", "timestamp")
    ]

    kafka_struct = F.struct(
        key_value.alias("key"),
        *[F.col(c).alias(c) for c in kafka_metadata_cols],
    )

    # make kafka struct a variant so map value types match header_map
    kafka_variant = F.try_parse_json(F.to_json(kafka_struct))

    kafka_map = F.map_from_arrays(F.array(F.lit("kafka")), F.array(kafka_variant))
    return F.map_concat(header_map, kafka_map)


def rate_limit(key, pdfs, state: GroupState):
    # combine non empty pdfs
    pdf_list = [pdf for pdf in pdfs if not pdf.empty]
    if not pdf_list:
        return iter(())  # must return empty iterator, not None

    # read existing state
    if state.exists:
        (state_last_ts,) = state.get
    else:
        state_last_ts = None

    batch = pd.concat(pdf_list, ignore_index=True).sort_values("ts")

    out_rows = []
    last_ts = state_last_ts

    for _, row in batch.iterrows():
        ts_val = int(row.ts)
        if last_ts is None or ts_val - last_ts >= 1000:
            last_ts = ts_val
            out_rows.append(row)

    # update state only if changed
    if last_ts != state_last_ts:
        state.update((last_ts,))

    # convert to DataFrames
    return (row.to_frame().T for row in out_rows)


def run():
    # print(get_offsets())
    spark = clients.spark()

    # Read from Kafka
    df = spark.readStream.format("kafka").options(**KAFKA_OPTIONS_READ).load()

    id_col = F.expr("uuid()").alias("id")

    ts_col = F.coalesce(
        (F.col("timestamp").cast("long") * 1000),
        (F.unix_timestamp(F.current_timestamp()) * 1000),
    ).alias("ts")

    metadata_col = build_metadata(df).alias("metadata")

    content_col = F.struct(
        (
            F.when(
                F.col("value").cast("string").rlike(r"^data:.*;base64,"),
                F.expr("substring(value, instr(value, ',') + 1, length(value))").cast(
                    "string"
                ),
            ).otherwise(F.col("value").cast("string"))
        ).alias("frame_image_b64")
    ).alias("content")

    source_df = df.select(
        id_col,
        ts_col,
        metadata_col,
        content_col,
    ).withColumn(
        "stream_id",
        F.coalesce(
            F.col("metadata.stream_id"),
            F.col("metadata.client_id"),
        ).cast("string"),
    )

    raw_df = (
        source_df.groupBy("stream_id")
        .applyInPandasWithState(
            rate_limit,
            outputStructType=source_df.schema,
            stateStructType="last_ts LONG",
            outputMode="append",
            timeoutConf=GroupStateTimeout.NoTimeout,
        )
        .drop("stream_id")
        .withColumn("metadata", F.to_json("metadata"))
        .withColumn("content", F.to_json("content"))
    )

    event_df = raw_df.select(
        F.lit(json.dumps({"store_id": "xyz"})).cast("binary").alias("key"),
        F.to_json(F.struct("*")).cast("binary").alias("value"),
    )
    write_query = (
        event_df.writeStream.format("kafka")
        .options(**KAFKA_OPTIONS_WRITE)
        .option(
            "checkpointLocation",
            "/Volumes/reggie_pierce/data/checkpoints/transform_test_v13",
        )
        .trigger(availableNow=True)
        .start()
    )

    write_query.awaitTermination()


def get_offsets():
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        security_protocol=SECURITY_PROTOCOL,
        enable_auto_commit=False,
    )

    partitions = consumer.partitions_for_topic(SOURCE_TOPIC)
    if partitions is None:
        raise RuntimeError("Topic not found")

    tps = [TopicPartition(SOURCE_TOPIC, p) for p in partitions]

    earliest = consumer.beginning_offsets(tps)
    latest = consumer.end_offsets(tps)

    return partitions, earliest, latest


def build_starting_offsets_json(latest):
    # Spark wants: {"topic": {"partition": offset}}
    structured = {
        SOURCE_TOPIC: {str(tp.partition): offset for tp, offset in latest.items()}
    }
    return json.dumps(structured, indent=2)


# simple script test
if __name__ == "__main__":
    os.environ.setdefault("DATABRICKS_CONFIG_PROFILE", "E2-DOGFOOD")
    run()
