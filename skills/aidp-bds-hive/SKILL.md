---
description: Read Hive tables from Oracle Big Data Service (BDS) over JDBC with Kerberos auth from an Oracle AI Data Platform Workbench notebook. Use when the user mentions BDS, Big Data Service, HiveServer2, HS2, hive2 JDBC, Kerberos keytab, principal, krb5.conf, or Hadoop on OCI. Auth = Kerberos via JAAS keytab login (no kinit binary required).
allowed-tools: Read, Write, Edit, Bash
---

# `aidp-bds-hive` — BDS HiveServer2 over JDBC + Kerberos

## When to use
- User has an OCI Big Data Service (BDS) cluster with HiveServer2 exposed on TCP 10000 / 10010 and a Kerberos keytab.
- User mentions: "BDS", "Big Data Service", "HiveServer2", "hive2 JDBC", "Kerberos keytab", "service principal", "krb5.conf", "kinit", "Hadoop on OCI".

## When NOT to use
- For a Hive metastore catalog read where the data lives on `oci://` and Hive only owns the metadata, prefer reading the underlying files via [`aidp-object-storage`](../aidp-object-storage/SKILL.md) or [`aidp-iceberg`](../aidp-iceberg/SKILL.md) and skip HS2 entirely.
- For non-Kerberos Hive (LDAP / NoAuth) — out of scope for this version. Use the generic JDBC pattern in [`aidp-jdbc-custom`](../aidp-jdbc-custom/SKILL.md) with `auth=ldap`.

## Network prerequisites — **read this first**

The AIDP cluster pod runs in a hidden VCN (typically `10.111.0.0/16`). Hive on BDS lives in the customer's VCN (often a `*.oraclevcn.com` private DNS zone). For this skill to work end-to-end **the customer must have the following in place before running**:

1. **VCN peering / DRG / NSG** between the AIDP workspace's hidden VCN and the BDS subnet, with TCP allowed on the HS2 port (10000 or 10010).
2. **Cross-VCN DNS** so the AIDP cluster can resolve the BDS hostname — `*.oraclevcn.com` is internal-only DNS by default. Either configure the OCI DNS resolver to forward the BDS zone, or use the BDS HS2 host's IP address directly in `HS2_HOST` and in the principal.
3. **KDC reachability.** The KDC hosts listed in your `krb5.conf` must also be reachable from the cluster on UDP/TCP 88.
4. **`kinit` is NOT required** on the cluster image — this skill uses Java's `Krb5LoginModule` (JAAS) directly. AIDP cluster images do not ship MIT Kerberos client binaries, so the `kinit`/`klist` path won't work; the JAAS path does.

If those aren't in place, run the example notebook from a workbench whose cluster IS peered to the BDS VCN. The skill code itself is environment-agnostic.

## Prerequisites in the notebook
1. Helpers on `sys.path` (run `aidp-connectors-bootstrap` first).
2. Keytab uploaded to a Volume or directly to `/tmp/` on the cluster. **Must be readable by the driver JVM**.
3. Env vars (or a notebook cell) with:
   ```
   BDS_HS2_HOST=nitishun0-0.rgroverprdpub1.rgroverprd.oraclevcn.com
   BDS_HS2_PORT=10010
   BDS_DATABASE=test
   BDS_TABLE=test_sample
   BDS_HS2_PRINCIPAL=hive/nitishun0-0.rgroverprdpub1.rgroverprd.oraclevcn.com@NITISH.ORACLE.COM
   BDS_KEYTAB_PATH=/tmp/hive.service.keytab
   BDS_KRB5_CONF=/tmp/krb5.conf            # optional — defaults to /etc/krb5.conf
   ```

## Auth: Kerberos via JAAS keytab login

```python
import os
from oracle_ai_data_platform_connectors.jdbc import (
    build_hive_jdbc_url,
    kerberos_login_via_jaas,
    runtime_load_hive_driver,
)

# 1. Make the Hive JDBC driver loadable in this Spark session.
#    Cluster has no `org.apache.hive.jdbc.HiveDriver` pre-installed, so we
#    fetch the standalone JAR from Maven Central and runtime-load it.
runtime_load_hive_driver(
    spark,
    jar_path="/tmp/hive-jdbc-standalone.jar",
    # maven_url=...   # override if you need a different Hive line
)

# 2. Log in with the keytab using Java's built-in Kerberos (no kinit binary).
#    The TGT lives in the JVM Subject and is auto-picked-up by the driver.
kerberos_login_via_jaas(
    spark,
    keytab_path=os.environ["BDS_KEYTAB_PATH"],
    principal=os.environ["BDS_HS2_PRINCIPAL"],
    krb5_conf_path=os.environ.get("BDS_KRB5_CONF"),  # None → /etc/krb5.conf
    # debug=True,    # turn on for KDC-trace logging
)

# 3. Build the URL.
url = build_hive_jdbc_url(
    hs2_host=os.environ["BDS_HS2_HOST"],
    hs2_port=int(os.environ.get("BDS_HS2_PORT", "10000")),
    database=os.environ["BDS_DATABASE"],
    hs2_principal=os.environ["BDS_HS2_PRINCIPAL"],
)
print(url)

# 4. Read.
df = (spark.read.format("jdbc")
      .option("url",       url)
      .option("driver",    "org.apache.hive.jdbc.HiveDriver")
      .option("dbtable",   os.environ["BDS_TABLE"])
      .option("fetchsize", "10000")
      .load())
df.show(10)
print("rows:", df.count())
```

## Why JAAS instead of `kinit`

`kinit -kt <keytab> <principal>` is the conventional Kerberos client login, but:

- AIDP cluster images don't ship MIT Kerberos client binaries (`kinit`, `klist`, `ktutil`). Calling them would fail with `FileNotFoundError`.
- Even if they did, the resulting credential cache lives at `/tmp/krb5cc_<uid>` and the Hive JDBC driver might run under a different UID inside the JVM.

`Krb5LoginModule` performs the same TGT exchange directly inside the JVM, stores the credentials in the current Subject, and the Hive JDBC driver picks them up automatically when it sees `auth=kerberos` in the URL. It is the supported pattern for Spark + Kerberos + Hive on every Spark distribution including DBR.

## Custom `krb5.conf`

If the cluster's `/etc/krb5.conf` doesn't list your BDS realm's KDCs (most clusters won't), upload your realm's `krb5.conf` to `/tmp/krb5.conf` and pass it as `krb5_conf_path`. A minimal working file for the principal in the example above:

```
[libdefaults]
  default_realm = NITISH.ORACLE.COM
  dns_lookup_realm = false
  dns_lookup_kdc = false
  ticket_lifetime = 24h
  renew_lifetime = 7d
  forwardable = true
  default_tgs_enctypes = aes128-cts-hmac-sha1-96 aes256-cts-hmac-sha1-96
  default_tkt_enctypes = aes128-cts-hmac-sha1-96 aes256-cts-hmac-sha1-96
  udp_preference_limit = 1

[domain_realm]
  .rgroverprdpub1.rgroverprd.oraclevcn.com = NITISH.ORACLE.COM
  rgroverprdpub1.rgroverprd.oraclevcn.com  = NITISH.ORACLE.COM

[realms]
  NITISH.ORACLE.COM = {
    admin_server = nitishmn0-0.rgroverprdpub1.rgroverprd.oraclevcn.com:749
    kdc          = nitishmn0-0.rgroverprdpub1.rgroverprd.oraclevcn.com:88
    kdc          = nitishmn1-0.rgroverprdpub1.rgroverprd.oraclevcn.com:88
  }
```

## Gotchas

- **`KrbException: Cannot locate KDC`** — your `krb5.conf` doesn't list KDCs reachable from the cluster, OR DNS for the KDC hosts isn't resolvable from the cluster. Fix the cross-VCN DNS or use IP addresses in `[realms]`.
- **`KrbException: Clock skew too great`** — keytab login requires < 5 min skew between cluster and KDC. AIDP clusters use NTP, but BDS hosts in unusual configurations may not. Re-sync time on whichever drifted.
- **`LoginException: Unable to obtain password from user`** — usually means the keytab principal doesn't match the `principal=` you passed. Verify with `klist -kt /path/to/keytab` on a host that has Kerberos tools.
- **`TTransportException: SASL ... GSS initiate failed`** — TGT acquisition succeeded but HS2 rejects it. Most common cause: HS2 service principal name in the URL (`principal=hive/host@REALM`) doesn't match what HS2 actually advertises. Also check that `auth=kerberos` is in the URL.
- **`No SocketFactory` / connection refused** — TCP path failed; the network prerequisites at the top of this skill are not in place. Run a `socket.create_connection((host, port), timeout=5)` probe to confirm.
- **`fetchsize=10000`** is a sensible default; smaller creates driver chatter, larger risks executor OOM.
- **Partitioned read** — for parallel reads, use `option("partitionColumn", ...).option("lowerBound", ...).option("upperBound", ...).option("numPartitions", N)`. Without these the read is single-partition.
- **`dbtable` accepts a subquery** — wrap in parens to push filters at the source: `option("dbtable", "(SELECT * FROM big_table WHERE dt > '2025-01-01') t")`.

## References

- Helper module: [scripts/oracle_ai_data_platform_connectors/jdbc/hive.py](../../scripts/oracle_ai_data_platform_connectors/jdbc/hive.py)
- Example notebook: [examples/bds_hive_kerberos.ipynb](../../examples/bds_hive_kerberos.ipynb)
- Live-test row: [tests/live-results/row07.json](../../tests/live-results/row07.json) — ship-as-is, customer-side validation
- BDS Kerberos overview: <https://docs.oracle.com/en-us/iaas/Content/bigdata/troubleshoot-cluster.htm>
- HiveServer2 JDBC client docs: <https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Clients>
