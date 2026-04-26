"""OCI Streaming via Spark structured streaming (Kafka-compat).

Default auth path is **SASL/PLAIN with an OCI auth token** — no custom JAR
needed. OAuthBearer is documented as Option B but requires a custom callback
handler the AIDP cluster image cannot easily ingest at the moment.

Critical AIDP gotcha: **streaming checkpoints must live under `/Volumes/...`**
— `/Workspace/...` (FUSE) and `oci://...` both fail silently.
"""

from __future__ import annotations

from typing import Optional


def bootstrap_for_region(region: str) -> str:
    """Return the OCI Streaming Kafka bootstrap broker for a region.

    Args:
        region: e.g. ``us-ashburn-1``.

    Returns:
        ``streaming.<region>.oci.oraclecloud.com:9092``.
    """
    return f"streaming.{region}.oci.oraclecloud.com:9092"


def build_kafka_options_sasl_plain(
    bootstrap_servers: str,
    tenancy_name: str,
    username: str,
    stream_pool_ocid: str,
    auth_token: str,
    topic: str,
    *,
    starting_offsets: str = "latest",
) -> dict:
    """Spark Kafka options for SASL/PLAIN with an OCI auth token.

    OCI Streaming's Kafka-compat surface expects the username in the form
    ``<tenancy_name>/<username>/<stream_pool_ocid>``. The password is the
    OCI auth token (Profile → Auth tokens → Generate Token).

    Args:
        bootstrap_servers: ``streaming.<region>.oci.oraclecloud.com:9092``.
        tenancy_name: OCI tenancy display name (NOT OCID).
        username: OCI user (typically email).
        stream_pool_ocid: ``ocid1.streampool.oc1...``.
        auth_token: 1-hour OCI auth token. Refresh before long jobs.
        topic: Kafka topic name (must exist in the stream pool).
        starting_offsets: ``latest`` | ``earliest`` | a JSON offset spec.

    Returns:
        Dict suitable for ``spark.readStream.format("kafka").options(**dict).load()``.
    """
    sasl_username = f"{tenancy_name}/{username}/{stream_pool_ocid}"
    jaas_config = (
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="{sasl_username}" '
        f'password="{auth_token}";'
    )
    return {
        "kafka.bootstrap.servers": bootstrap_servers,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": jaas_config,
        "subscribe": topic,
        "startingOffsets": starting_offsets,
    }


def build_kafka_options_oauthbearer(
    bootstrap_servers: str,
    token_endpoint_url: str,
    callback_handler_class: str,
    topic: str,
    *,
    starting_offsets: str = "latest",
) -> dict:
    """Spark Kafka options for SASL_SSL OAuthBearer (Option B, requires custom JAR).

    NOT RECOMMENDED on AIDP without a pre-attached callback-handler JAR — the
    cluster image doesn't ship one out-of-the-box. Documented for completeness
    so users with a packaged JAR can use it.

    Args:
        bootstrap_servers: same as SASL/PLAIN.
        token_endpoint_url: OAuth2 token endpoint
            (e.g. ``https://auth.<region>.oraclecloud.com/v1/oauth2/token``).
        callback_handler_class: FQCN of a class implementing
            ``OAuthBearerLoginCallbackHandler`` and bundled in a JAR attached
            to the cluster.
        topic: Kafka topic name.
        starting_offsets: ``latest`` | ``earliest`` | offsets JSON.
    """
    return {
        "kafka.bootstrap.servers": bootstrap_servers,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "OAUTHBEARER",
        "kafka.sasl.oauthbearer.token.endpoint.url": token_endpoint_url,
        "kafka.sasl.login.callback.handler.class": callback_handler_class,
        "subscribe": topic,
        "startingOffsets": starting_offsets,
    }


def validate_checkpoint_path(path: str) -> str:
    """Validate that a Spark streaming checkpoint path is AIDP-compatible.

    AIDP-compatible = ``/Volumes/<catalog>/<schema>/<volume>/...``. The
    ``/Workspace/`` mount (FUSE) and ``oci://`` URIs both fail silently for
    streaming checkpoints.

    Returns:
        ``path`` unchanged if valid.

    Raises:
        ValueError: If path is on /Workspace or starts with oci://.
    """
    p = path.strip()
    if p.startswith("/Workspace") or p.startswith("/workspace"):
        raise ValueError(
            "Streaming checkpoints cannot live on /Workspace (FUSE). "
            "Use /Volumes/<catalog>/<schema>/<volume>/_checkpoints/... "
            "instead."
        )
    if p.startswith("oci://"):
        raise ValueError(
            "Streaming checkpoints cannot use oci:// directly. "
            "Use /Volumes/... instead."
        )
    if not p.startswith("/Volumes/"):
        raise ValueError(
            f"Streaming checkpoint should live under /Volumes/...; got {p!r}"
        )
    return p
