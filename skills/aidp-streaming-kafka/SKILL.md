---
description: Consume an OCI Streaming stream from an AIDP notebook via Spark structured streaming (Kafka-compat). Use when the user mentions OCI Streaming, Kafka on OCI, stream pool, structured streaming, or wants to read Kafka messages into Spark. Default auth is SASL/PLAIN with an OCI auth token. Critical gotcha: checkpoints MUST live under /Volumes/, not /Workspace/.
allowed-tools: Read, Write, Edit, Bash
---

# `aidp-streaming-kafka` — OCI Streaming via Spark structured streaming

## When to use
- User wants to consume an OCI Streaming stream (Kafka-compat) from an AIDP notebook.
- User mentions: "OCI Streaming", "Kafka on OCI", "stream pool", "structured streaming", "Kafka topic".

## When NOT to use
- For batch reads of files in OCI Object Storage → standard `spark.read.format("csv"|"parquet").load("oci://...")` is fine without this skill.
- For other Kafka deployments (Confluent, MSK) — same Spark Kafka API works; just point `bootstrap.servers` at the right broker and skip the OCI-specific username format.

## Prerequisites in the AIDP notebook
1. Spark Kafka connector on the cluster (`spark-sql-kafka-0-10_<scala>:<spark>` — AIDP's `tpcds` cluster has this).
2. Helpers on `sys.path`.
3. OCI Streaming **stream pool OCID** + region.
4. An OCI **auth token** (Profile → Auth tokens → Generate Token in the OCI console). 1-hour TTL — refresh before any job that runs longer than that.
5. A **Volumes-mounted checkpoint location** (`/Volumes/<catalog>/<schema>/<volume>/_checkpoints/...`). **Do NOT use `/Workspace/...` — the streaming engine fails silently.**

## Auth: pick one

### Option A — SASL/PLAIN with OCI auth token (recommended default)

```python
import os
from oracle_ai_data_platform_connectors.streaming import (
    bootstrap_for_region, build_kafka_options_sasl_plain,
    validate_checkpoint_path,
)

bootstrap = bootstrap_for_region(os.environ["OCI_REGION"])

opts = build_kafka_options_sasl_plain(
    bootstrap_servers=bootstrap,
    tenancy_name=os.environ["OCI_TENANCY_NAME"],     # display name, not OCID
    username=os.environ["OCI_USERNAME"],             # OCI user email
    stream_pool_ocid=os.environ["OCI_STREAM_POOL_OCID"],
    auth_token=os.environ["OCI_AUTH_TOKEN"],         # 1h TTL — refresh before long jobs
    topic=os.environ["KAFKA_TOPIC"],
)

raw = spark.readStream.format("kafka").options(**opts).load()

# Validate checkpoint path BEFORE starting (saves you from silent FUSE failures)
checkpoint = validate_checkpoint_path(os.environ["KAFKA_CHECKPOINT_VOLUME"])

query = (
    raw.writeStream.format("delta")
       .outputMode("append")
       .option("checkpointLocation", checkpoint)
       .toTable("my_catalog.my_schema.my_table")
)
query.awaitTermination(timeout=120)
print("input rows in last batch:", query.lastProgress.get("numInputRows"))
```

### Option B — SASL_SSL OAuthBearer (NOT recommended on AIDP today)

OCI Streaming Kafka supports OAuthBearer, but Spark needs a custom `OAuthBearerLoginCallbackHandler` JAR that AIDP's cluster image doesn't ship. Use only if you have a packaged JAR attached via Cluster → Libraries.

```python
from oracle_ai_data_platform_connectors.streaming import build_kafka_options_oauthbearer

opts = build_kafka_options_oauthbearer(
    bootstrap_servers=bootstrap,
    token_endpoint_url=f"https://auth.{os.environ['OCI_REGION']}.oraclecloud.com/v1/oauth2/token",
    callback_handler_class="com.example.MyOAuthBearerCallbackHandler",
    topic=os.environ["KAFKA_TOPIC"],
)
```

## Gotchas
- **Checkpoint path** — must be `/Volumes/...`. The `validate_checkpoint_path()` helper raises a `ValueError` if you pass `/Workspace/...` or `oci://...`. This is the #1 cause of "stream runs but no data appears" complaints in AIDP.
- **Auth token TTL = 1 hour.** For longer runs, plan to checkpoint, stop the stream, refresh the token, restart from checkpoint. RP-based Kafka SASL (`com.oracle.bmc.auth.sasl.ResourcePrincipalsLoginModule`) is blocked at the AIDP platform level (RP tokens not provided).
- **Username format** — `<tenancy_name>/<username>/<stream_pool_ocid>`. Tenancy *name* (display name), NOT tenancy OCID.
- **Streaming jobs run forever.** The AIDP workflow timeout doesn't apply once a streaming query is started. Set `Max Concurrent Runs = 1` on the wrapping job.
- **Schema** — the `kafka` source returns `(key, value, topic, partition, offset, timestamp, timestampType)`. Cast `value` to STRING and parse JSON / Avro yourself downstream.

## References
- Helpers: [scripts/oracle_ai_data_platform_connectors/streaming/kafka.py](../../scripts/oracle_ai_data_platform_connectors/streaming/kafka.py)
- OCI Streaming Kafka compat: https://docs.oracle.com/en-us/iaas/Content/Streaming/Tasks/kafkacompatibility_topic-Configuration.htm
