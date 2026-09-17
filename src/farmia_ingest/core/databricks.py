"""Acceso a `dbutils` fuera del notebook (para los entrypoints/CLI).

En un notebook, `dbutils` es global. En un entry point (wheel/CLI) hay que obtenerlo. Se intenta de
forma perezosa y tolerante: si no estamos en Databricks, devuelve None (y el batch se ejecuta sin
archivado; el streaming, que necesita secretos, avisará).
"""
from __future__ import annotations


def get_dbutils(spark):
    try:
        from pyspark.dbutils import DBUtils  # disponible en Databricks

        return DBUtils(spark)
    except Exception:
        try:
            import IPython  # dentro de un notebook

            ip = IPython.get_ipython()
            return ip.user_ns.get("dbutils") if ip else None
        except Exception:
            return None
