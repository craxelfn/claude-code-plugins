"""Oracle AI Data Platform Spark connectors helper package.

Importable from an AIDP notebook after the user adds the plugin's scripts/
directory to sys.path. Public surface is intentionally small; each connector
skill points users at one or two helpers below.

Submodules:
    auth      - wallet, dbtoken, oci_config, user_principal, secrets
    jdbc      - oracle (ALH/ATP/ExaCS), hive (BDS)
    rest      - fusion, epm, essbase
    streaming - kafka
"""

__version__ = "0.1.0"

__all__ = [
    "auth",
    "jdbc",
    "rest",
    "streaming",
]
