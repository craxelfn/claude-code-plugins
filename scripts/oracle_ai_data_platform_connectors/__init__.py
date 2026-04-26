"""Oracle AI Data Platform Spark connectors helper package.

Importable from an AIDP notebook after the user adds the plugin's scripts/
directory to sys.path. Public surface is intentionally small; each connector
skill points users at one or two helpers below.

Submodules:
    auth            - wallet, dbtoken, oci_config, user_principal, secrets
    jdbc            - oracle (ALH/ATP/ExaCS), hive (BDS)
    rest            - fusion, epm, essbase
    streaming       - kafka
    aidataplatform  - builder for the AIDP `aidataplatform` Spark format
                      (ORACLE_DB, ORACLE_EXADATA, ORACLE_ALH, ORACLE_ATP,
                      POSTGRESQL, MYSQL, MYSQL_HEATWAVE, SQLSERVER, HIVE,
                      KAFKA, FUSION_BICC, GENERIC_REST)
"""

__version__ = "0.2.0"

from .aidataplatform import AIDP_FORMAT, aidataplatform_options

__all__ = [
    "auth",
    "jdbc",
    "rest",
    "streaming",
    "AIDP_FORMAT",
    "aidataplatform_options",
]
