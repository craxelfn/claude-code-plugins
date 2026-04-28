"""Hive JDBC connectivity for Big Data Service (BDS) clusters.

Two pieces:

* ``build_hive_jdbc_url`` constructs the HiveServer2 JDBC URL with Kerberos
  options.

* ``kerberos_login_via_jaas`` performs a keytab-based Kerberos login using the
  JVM's built-in ``Krb5LoginModule`` and a JAAS config file written to
  ``/tmp/``. This avoids the need for the MIT Kerberos client binaries
  (``kinit``/``klist``), which AIDP cluster images do NOT ship. The TGT lives
  in the JVM's Subject and is automatically picked up by the Hive JDBC driver
  when ``auth=kerberos`` is in the URL.

* ``runtime_load_hive_driver`` downloads the Hive JDBC uber-jar from Maven
  Central and registers it via the runtime-load pattern (URLClassLoader +
  ``DriverManager.registerDriver`` + executor distribution).

Network prerequisites — these are NOT enforced in code; the customer must
ensure them before running:

* The cluster pod CIDR must have L3 reachability to the BDS HS2 host on the
  HS2 port (commonly 10000 or 10010). This means VCN peering / DRG / NSG
  rules between the AIDP workspace's hidden VCN and the BDS subnet.
* DNS for the BDS host must resolve from the cluster. ``*.oraclevcn.com``
  hostnames only resolve inside their own VCN; either set up cross-VCN DNS
  resolution or use the BDS host's IP address directly in
  ``hs2_host``/``hs2_principal``.
* The KDC host(s) listed in your ``krb5.conf`` must also be reachable from
  the cluster on UDP/TCP 88.

If the AIDP cluster cannot reach the BDS subnet, this skill ships as-is and
the customer runs the example notebook from a workbench whose cluster is
peered with the BDS VCN.
"""

from __future__ import annotations

import os
from typing import Optional


# ---------------------------------------------------------------------------
# JDBC URL
# ---------------------------------------------------------------------------

def build_hive_jdbc_url(
    *,
    hs2_host: str,
    hs2_port: int = 10000,
    database: str = "default",
    hs2_principal: Optional[str] = None,
    transport_mode: str = "binary",
    extra_props: Optional[dict] = None,
) -> str:
    """Construct a HiveServer2 JDBC URL.

    Args:
        hs2_host: HiveServer2 hostname or IP, e.g.
            ``nitishun0-0.rgroverprdpub1.rgroverprd.oraclevcn.com``.
        hs2_port: HS2 port. Default 10000; some BDS clusters expose 10010.
        database: Hive database to connect to. Default ``"default"``.
        hs2_principal: Service principal for Kerberos auth, e.g.
            ``hive/<host>@<REALM>``. If provided, ``auth=kerberos`` is added.
        transport_mode: ``"binary"`` (default; TCP) or ``"http"`` (HS2 over
            HTTP, behind a load balancer).
        extra_props: Extra ``;key=value`` pairs to append to the URL.

    Returns:
        A JDBC URL string suitable for ``spark.read.format("jdbc")
        .option("url", ...)``.
    """
    url = f"jdbc:hive2://{hs2_host}:{hs2_port}/{database}"
    props = {}
    if hs2_principal:
        props["principal"] = hs2_principal
        props["auth"] = "kerberos"
    if transport_mode and transport_mode != "binary":
        props["transportMode"] = transport_mode
    if extra_props:
        props.update(extra_props)
    if props:
        url = url + ";" + ";".join(f"{k}={v}" for k, v in props.items())
    return url


# ---------------------------------------------------------------------------
# Kerberos login via JAAS (no `kinit` binary required)
# ---------------------------------------------------------------------------

_JAAS_CONFIG_TEMPLATE = """KrbLogin {{
    com.sun.security.auth.module.Krb5LoginModule required
    useKeyTab=true
    keyTab="{keytab}"
    principal="{principal}"
    storeKey=true
    doNotPrompt=true
    debug={debug};
}};
"""


def kerberos_login_via_jaas(
    spark,
    *,
    keytab_path: str,
    principal: str,
    krb5_conf_path: Optional[str] = None,
    jaas_path: str = "/tmp/aidp_hive_jaas.conf",
    debug: bool = False,
) -> str:
    """Log in via Java's built-in Kerberos using a keytab + JAAS config.

    This is the pure-Java equivalent of ``kinit -kt <keytab> <principal>``.
    Works without the MIT Kerberos client binaries.

    The function:

    1. Writes a JAAS config to ``jaas_path`` referencing the keytab + principal.
    2. Sets the JVM system properties ``java.security.auth.login.config``,
       ``java.security.krb5.conf``, ``javax.security.auth.useSubjectCredsOnly``,
       ``sun.security.krb5.debug``.
    3. Calls ``LoginContext("KrbLogin").login()`` — the resulting Subject is
       cached internally by the JVM and used by the Hive JDBC driver when
       ``auth=kerberos`` is in the URL.

    Args:
        spark: The active ``SparkSession`` (used to reach the JVM gateway).
        keytab_path: Filesystem path to the keytab. Must be readable by the
            driver JVM. ``/tmp/<file>.keytab`` is the recommended pattern.
        principal: Kerberos principal in the keytab, e.g.
            ``hive/<host>@<REALM>``.
        krb5_conf_path: Optional path to a custom ``krb5.conf``. If omitted,
            the JVM uses ``/etc/krb5.conf``. Override when the system file
            doesn't list the BDS realm's KDCs.
        jaas_path: Where to write the generated JAAS config. Default
            ``/tmp/aidp_hive_jaas.conf``.
        debug: If True, set ``sun.security.krb5.debug=true`` and JAAS
            ``debug=true`` — emits Kerberos-trace lines to stderr.

    Returns:
        ``jaas_path`` for chaining / debugging.

    Raises:
        Java ``LoginException`` (propagates as ``Py4JJavaError``) when the
        keytab/principal pair is wrong, the keytab file isn't readable, the
        KDC is unreachable, or clock skew > 5 minutes.
    """
    if not os.path.exists(keytab_path):
        raise FileNotFoundError(f"keytab not found: {keytab_path}")

    config_text = _JAAS_CONFIG_TEMPLATE.format(
        keytab=keytab_path,
        principal=principal,
        debug=str(debug).lower(),
    )

    fd = os.open(jaas_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    with os.fdopen(fd, "w") as f:
        f.write(config_text)

    jvm = spark._jvm
    System = jvm.java.lang.System
    System.setProperty("java.security.auth.login.config", jaas_path)
    if krb5_conf_path:
        System.setProperty("java.security.krb5.conf", krb5_conf_path)
    System.setProperty("javax.security.auth.useSubjectCredsOnly", "false")
    if debug:
        System.setProperty("sun.security.krb5.debug", "true")

    LoginContext = jvm.javax.security.auth.login.LoginContext
    ctx = LoginContext("KrbLogin")
    ctx.login()
    return jaas_path


# ---------------------------------------------------------------------------
# Runtime-load Hive JDBC driver
# ---------------------------------------------------------------------------

# Maven Central coordinates for the Hive JDBC standalone uber-jar. This single
# JAR includes the Hive client + Thrift + the small bits of Hadoop the driver
# needs at JDBC time. Customers on a different Hive line can pass their own
# coordinates.
_DEFAULT_HIVE_JDBC_VERSION = "3.1.3"
_DEFAULT_HIVE_JDBC_URL = (
    "https://repo1.maven.org/maven2/org/apache/hive/hive-jdbc/"
    f"{_DEFAULT_HIVE_JDBC_VERSION}/hive-jdbc-{_DEFAULT_HIVE_JDBC_VERSION}-standalone.jar"
)


def runtime_load_hive_driver(
    spark,
    *,
    jar_path: str = "/tmp/hive-jdbc-standalone.jar",
    maven_url: str = _DEFAULT_HIVE_JDBC_URL,
    distribute_to_executors: bool = True,
) -> str:
    """Download Hive JDBC standalone JAR from Maven Central and runtime-load it.

    Wraps :func:`download_jdbc_jar` + :func:`add_jdbc_jar_at_runtime` for the
    BDS Hive case. Driver class is ``org.apache.hive.jdbc.HiveDriver``.

    Args:
        spark: Active ``SparkSession``.
        jar_path: Where to write the downloaded JAR. Must be JVM-readable.
        maven_url: Override the default Maven URL if you need a different
            Hive line. Pin the version that matches your BDS cluster.
        distribute_to_executors: If True (default) push the JAR to executors
            via ``SparkContext.addJar`` so partitioned reads work.

    Returns:
        ``jar_path`` for chaining.
    """
    from .runtime_load import add_jdbc_jar_at_runtime, download_jdbc_jar

    download_jdbc_jar(maven_url=maven_url, target_path=jar_path)
    add_jdbc_jar_at_runtime(
        spark,
        jar_path=jar_path,
        driver_class="org.apache.hive.jdbc.HiveDriver",
        distribute_to_executors=distribute_to_executors,
    )
    return jar_path
