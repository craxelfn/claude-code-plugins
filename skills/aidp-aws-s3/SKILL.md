---
description: Read and write AWS S3 (`s3a://`) from an AIDP notebook. Use when the user mentions S3, AWS S3 bucket, s3a, or has AWS access keys. Auth is access key + secret key via the Hadoop S3A connector. boto3 is also available for non-Spark management operations (list, copy).
allowed-tools: Read, Write, Edit, Bash
---

# `aidp-aws-s3` — AWS S3 via the S3A connector

Read or write `s3a://<bucket>/<key>` paths from AIDP Spark using AWS access keys. Optional `boto3` path for management operations (list, copy, head).

## When to use
- AIDP needs to consume or land data in AWS S3.
- Mentioned: "S3", "s3a", "AWS bucket".

## When NOT to use
- For OCI Object Storage → [`aidp-object-storage`](../aidp-object-storage/SKILL.md).
- For Azure ADLS Gen2 → [`aidp-azure-adls`](../aidp-azure-adls/SKILL.md).

## Cluster prerequisite — `aws-java-sdk-bundle`

The S3A driver needs `hadoop-aws` (typically already in the cluster) plus `aws-java-sdk-bundle-<ver>.jar` (~280 MB). If not present, upload the matching version to a Volume and attach via the cluster Library tab. Mismatched versions silently fail with NoSuchMethod errors at runtime.

## Spark read/write (S3A, key-based auth)

```python
import os

# Tell the S3A driver to use simple-credential auth from the standard env vars.
spark.conf.set("fs.s3a.aws.credentials.provider",
               "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

# Standard AWS env vars are read by the SimpleAWSCredentialsProvider.
os.environ["AWS_ACCESS_KEY_ID"]     = os.environ["S3_ACCESS_KEY"]
os.environ["AWS_SECRET_ACCESS_KEY"] = os.environ["S3_SECRET_KEY"]

bucket = os.environ["S3_BUCKET"]
key    = os.environ["S3_FILE"]

# Read JSON (swap .json for .csv / .parquet / .delta as needed)
df = spark.read.json(f"s3a://{bucket}/{key}")
df.show()

# Write to a managed table
df.write.mode("overwrite").format("delta").saveAsTable("default.default.data_from_s3")
```

## boto3 fallback (management ops, not data plane)

```python
import boto3, os

s3 = boto3.client(
    "s3",
    aws_access_key_id     = os.environ["S3_ACCESS_KEY"],
    aws_secret_access_key = os.environ["S3_SECRET_KEY"],
    region_name           = os.environ.get("S3_REGION", "us-east-1"),
)

resp = s3.list_objects_v2(Bucket=os.environ["S3_BUCKET"], Prefix="")
for obj in resp.get("Contents", []):
    print(obj["Key"])
```

## Gotchas
- **Use `s3a://` (the Hadoop driver), not `s3://` or `s3n://`.** The latter two are deprecated and may not be present in the cluster.
- **`aws-java-sdk-bundle` version drift** — pin to the version `hadoop-aws` was built against. Lab clusters often need this jar installed; the symptom of mismatch is `NoSuchMethodError` deep in `org.apache.hadoop.fs.s3a` when listing/reading.
- **Secrets in env vars only.** Never hard-code keys in notebooks. Source from `.env`/OCI Vault.
- **Region** — `boto3.client('s3', region_name=...)` is required for non-default regions; for the Spark path the bucket region is auto-discovered, but you may need `fs.s3a.endpoint=s3.<region>.amazonaws.com` for non-us-east-1 if listings fail.
- **Egress cost & latency** — S3 reads from AIDP cross-cloud. For heavy ETL, copy to OCI Object Storage once and read locally.

## References
- Official sample: [oracle-samples/oracle-aidp-samples → `data-engineering/ingestion/Ingest_from_Multi_Cloud.ipynb`](https://github.com/oracle-samples/oracle-aidp-samples/blob/main/data-engineering/ingestion/Ingest_from_Multi_Cloud.ipynb)
- Hadoop AWS docs: <https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/>
