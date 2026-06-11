from pyspark.sql import SparkSession
from pyspark.sql.functions import substring_index
from pyspark.sql.functions import (
    col, from_json, window, count, avg,
    sum as _sum, when, lit, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, BooleanType, IntegerType
)


KAFKA_BOOTSTRAP   = "localhost:9092"
KAFKA_TOPIC       = "wikimedia"
INFLUX_HOST       = "localhost"
INFLUX_PORT       = 8086
INFLUX_DB         = "wikimedia"
TRIGGER_INTERVAL  = "10 seconds"
CHECKPOINT_RAW    = "/tmp/spark_checkpoints/wikimedia_raw"
CHECKPOINT_AGG    = "/tmp/spark_checkpoints/wikimedia_agg"


spark = SparkSession.builder \
    .master("local[*]") \
    .appName("wikimedia_streaming") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

meta_schema = StructType([
    StructField("uri",        StringType(),  True),
    StructField("request_id", StringType(),  True),
    StructField("id",         StringType(),  True),
    StructField("domain",     StringType(),  True),
    StructField("stream",     StringType(),  True),
    StructField("dt",         StringType(),  True),
    StructField("topic",      StringType(),  True),
    StructField("partition",  IntegerType(), True),
    StructField("offset",     LongType(),    True),
])

length_schema = StructType([
    StructField("old", IntegerType(), True),
    StructField("new", IntegerType(), True),
])

revision_schema = StructType([
    StructField("old", LongType(), True),
    StructField("new", LongType(), True),
])

event_schema = StructType([
    StructField("$schema",            StringType(),    True),
    StructField("meta",               meta_schema,     True),
    StructField("id",                 LongType(),      True),
    StructField("type",               StringType(),    True),
    StructField("namespace",          IntegerType(),   True),
    StructField("title",              StringType(),    True),
    StructField("title_url",          StringType(),    True),
    StructField("comment",            StringType(),    True),
    StructField("parsedcomment",      StringType(),    True),
    StructField("timestamp",          LongType(),      True),
    StructField("user",               StringType(),    True),
    StructField("bot",                BooleanType(),   True),
    StructField("minor",              BooleanType(),   True),
    StructField("patrolled",          BooleanType(),   True),
    StructField("notify_url",         StringType(),    True),
    StructField("length",             length_schema,   True),
    StructField("revision",           revision_schema, True),
    StructField("server_url",         StringType(),    True),
    StructField("server_name",        StringType(),    True),
    StructField("server_script_path", StringType(),    True),
    StructField("wiki",               StringType(),    True),
])

raw_kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

parsed_df = (
     raw_kafka_df
     .selectExpr("CAST(value AS STRING) AS raw")
     .withColumn("json_str", 
         when(col("raw").startswith("data: "), 
             col("raw").substr(lit(7), lit(99999)))
         .otherwise(lit(""))
     )
     .filter(col("json_str") != "")
     .select(from_json(col("json_str"), event_schema).alias("d"))
     .select(
         col("d.id").alias("id"),
         col("d.type").alias("type"),
         col("d.namespace").alias("namespace"),
         col("d.title").alias("title"),
         col("d.user").alias("user"),
         col("d.bot").alias("bot"),
         col("d.minor").alias("minor"),
         col("d.patrolled").alias("patrolled"),
         col("d.timestamp").alias("timestamp"),
         col("d.comment").alias("comment"),
         col("d.wiki").alias("wiki"),
         col("d.server_name").alias("server_name"),
         col("d.length.old").alias("length_old"),
         col("d.length.new").alias("length_new"),
         col("d.revision.old").alias("revision_old"),
         col("d.revision.new").alias("revision_new"),
         col("d.meta.domain").alias("domain"),
         col("d.meta.dt").alias("dt"),
         col("d.meta.stream").alias("stream"),
     )
     .dropna(subset=["id", "timestamp"])
     .withColumn("event_time", to_timestamp(col("dt")))
     .withColumn(
         "length_delta",
         when(
             col("length_old").isNotNull() & col("length_new").isNotNull(),
             col("length_new") - col("length_old")
         ).otherwise(lit(0))
     )
 )

agg_df = (
     parsed_df
     .filter(col("event_time").isNotNull())
     .withWatermark("event_time", "2 minutes")
     .groupBy(
         window(col("event_time"), "1 minute").alias("w"),
         col("wiki"),
         col("type"),
     )
     .agg(
         count("*").alias("edit_count"),
         _sum("length_delta").alias("total_length_delta"),
         avg("length_delta").alias("avg_length_delta"),
         count(when(col("bot") == True, 1)).alias("bot_edit_count"),
         count(when(col("minor") == True, 1)).alias("minor_edit_count"),
     )
     .withColumn("window_start", col("w.start"))
     .drop("w")
 )


def write_raw_batch(batch_df, batch_id):
    """Write raw edit events from one micro-batch to InfluxDB."""
    from influxdb import InfluxDBClient

    rows = batch_df.collect()
    if not rows:
        return

    client = InfluxDBClient(host=INFLUX_HOST, port=INFLUX_PORT, database=INFLUX_DB)
    points = []

    for row in rows:
        length_old   = int(row.length_old)   if row.length_old   is not None else 0
        length_new   = int(row.length_new)   if row.length_new   is not None else 0
        revision_old = int(row.revision_old) if row.revision_old is not None else 0
        revision_new = int(row.revision_new) if row.revision_new is not None else 0

        points.append({
            "measurement": "page_edits",
            "tags": {
                "wiki":        row.wiki        or "unknown",
                "domain":      row.domain      or "unknown",
                "server_name": row.server_name or "unknown",
                "type":        row.type        or "unknown",
                "bot":         str(row.bot),
                "minor":       str(row.minor),
                "patrolled":   str(row.patrolled),
                "namespace":   str(row.namespace),
            },
            "time": int(row.timestamp) * 1_000_000_000,
            "fields": {
                "id":           int(row.id),
                "title":        row.title   or "",
                "comment":      row.comment or "",
                "length_old":   length_old,
                "length_new":   length_new,
                "length_delta": length_new - length_old,
                "revision_old": revision_old,
                "revision_new": revision_new,
                "user":        row.user        or "anonymous",
            },
        })

    client.write_points(points, batch_size=500, time_precision="n")
    client.close()
    print(f"[raw]  batch {batch_id}: wrote {len(points)} points to InfluxDB")


def write_agg_batch(batch_df, batch_id):
    """Write 1-minute aggregates from one micro-batch to InfluxDB."""
    from influxdb import InfluxDBClient

    rows = batch_df.collect()
    if not rows:
        return

    client = InfluxDBClient(host=INFLUX_HOST, port=INFLUX_PORT, database=INFLUX_DB)
    points = []

    for row in rows:
        if row.window_start is None:
            continue
        points.append({
            "measurement": "edit_stats_1m",
            "tags": {
                "wiki": row.wiki or "unknown",
                "type": row.type or "unknown",
            },
            "time": int(row.window_start.timestamp()) * 1_000_000_000,
            "fields": {
                "edit_count":         int(row.edit_count),
                "total_length_delta": int(row.total_length_delta or 0),
                "avg_length_delta":   float(row.avg_length_delta or 0.0),
                "bot_edit_count":     int(row.bot_edit_count),
                "minor_edit_count":   int(row.minor_edit_count),
            },
        })

    if points:
        client.write_points(points, batch_size=500, time_precision="n")
    client.close()
    print(f"[agg]  batch {batch_id}: wrote {len(points)} windows to InfluxDB")


from influxdb import InfluxDBClient as _IC
_IC(host=INFLUX_HOST, port=INFLUX_PORT).create_database(INFLUX_DB)
print(f"InfluxDB database '{INFLUX_DB}' ready.")


raw_query = (
     parsed_df.writeStream
     .outputMode("append")
     .foreachBatch(write_raw_batch)
     .option("checkpointLocation", CHECKPOINT_RAW)
     .trigger(processingTime=TRIGGER_INTERVAL)
     .start()
 )

agg_query = (
     agg_df.writeStream
     .outputMode("update")
     .foreachBatch(write_agg_batch)
     .option("checkpointLocation", CHECKPOINT_AGG)
     .trigger(processingTime=TRIGGER_INTERVAL)
     .start()
 )

print(f"Streaming started. Trigger interval: {TRIGGER_INTERVAL}")
print("Press Ctrl+C to stop.\n")

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\nShutting down streams …")
    raw_query.stop()
    agg_query.stop()
    spark.stop()
    print("Done.")