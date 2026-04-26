"""Hive JDBC URL + Spark options for BDS HiveServer2.

LDAP is the v0.1 default; Kerberos is supported but requires the AIDP cluster
image to ship MIT Kerberos client (`kinit`), which is unverified at the time
of writing — the live BDS test (row 10 of the matrix) is what proves it.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from typing import Optional

HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver"


def build_hive_jdbc_url(
    host: str,
    port: int = 10000,
    database: str = "default",
    auth: str = "ldap",
    principal: Optional[str] = None,
    transport_mode: Optional[str] = None,
    ssl: bool = False,
) -> str:
    """Build a HiveServer2 JDBC URL.

    Args:
        host: HS2 hostname (BDS-internal, often a private IP).
        port: HS2 port. Default 10000 (plain). Use 10001 for SSL/HTTP.
        database: Default schema.
        auth: ``ldap``, ``kerberos``, or ``noSasl`` (dev only).
        principal: Required when ``auth=kerberos``. Format
            ``hive/<host>@<REALM>``.
        transport_mode: Optional ``http`` for HTTP transport.
        ssl: If True, append ``;ssl=true``.

    Returns:
        ``jdbc:hive2://<host>:<port>/<database>;auth=<auth>[;principal=...][;ssl=true]``

    Raises:
        ValueError: kerberos auth without a principal.
    """
    auth = auth.lower()
    if auth == "kerberos" and not principal:
        raise ValueError(
            "Kerberos auth requires `principal` (e.g. 'hive/<host>@<REALM>')"
        )

    parts = [f"jdbc:hive2://{host}:{port}/{database};auth={auth}"]
    if auth == "kerberos":
        parts.append(f"principal={principal}")
    if transport_mode:
        parts.append(f"transportMode={transport_mode}")
    if ssl:
        parts.append("ssl=true")
    return ";".join(parts)


def spark_hive_jdbc_options(
    url: str,
    user: Optional[str] = None,
    password: Optional[str] = None,
    *,
    fetchsize: int = 10_000,
) -> dict:
    """Spark JDBC options for Hive.

    For LDAP, pass ``user``/``password``. For Kerberos, leave them None — the
    URL's ``;auth=kerberos;principal=...`` and a prior ``kinit`` cover auth.
    """
    opts: dict = {
        "url": url,
        "driver": HIVE_DRIVER,
        "fetchsize": str(fetchsize),
    }
    if user is not None:
        opts["user"] = user
    if password is not None:
        opts["password"] = password
    return opts


def kerberos_kinit(
    principal: str,
    keytab_path: str,
) -> None:
    """Run ``kinit -kt <keytab> <principal>`` to obtain a Kerberos TGT.

    Args:
        principal: Full Kerberos principal, e.g. ``user@EXAMPLE.COM``.
        keytab_path: Path to the keytab file. Must live under /tmp (FUSE
            permissions will not let the kinit process read /Workspace).

    Raises:
        FileNotFoundError: If ``kinit`` isn't on PATH (AIDP cluster image
            doesn't include MIT Kerberos client).
        subprocess.CalledProcessError: If kinit returns non-zero.
    """
    if shutil.which("kinit") is None:
        raise FileNotFoundError(
            "kinit not found on PATH. The AIDP cluster image may not "
            "include MIT Kerberos client; use LDAP auth instead, or ask "
            "Oracle support to add krb5-user to the cluster image."
        )

    keytab = Path(keytab_path)
    if not keytab.exists():
        raise FileNotFoundError(f"keytab not found: {keytab}")
    if not str(keytab.resolve()).startswith("/tmp"):
        raise ValueError("keytab must live under /tmp (FUSE caveats)")

    subprocess.run(
        ["kinit", "-kt", str(keytab), principal],
        check=True,
        capture_output=True,
        text=True,
    )
