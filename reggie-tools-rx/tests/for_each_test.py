import asyncio
from reggie_tools import clients, configs


if __name__ == "__main__":
    config = configs.get()
    spark = clients.spark(config)
    subject = asyncio.Queue()

    def _publish_batch(df, batch_id):
        rows = [r.asDict() for r in df.collect()]
        print(rows)
        for r in rows:
            subject.put_nowait(r)

    stream_df = spark.readStream.format("delta").table(
        "reggie_pierce.iot_ingest.detections"
    )

    query = (
        stream_df.writeStream.foreachBatch(_publish_batch).outputMode("append").start()
    )

    query.awaitTermination()
