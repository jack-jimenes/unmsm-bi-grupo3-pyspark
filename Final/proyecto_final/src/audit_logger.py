"""Módulo de auditoría en Parquet para el pipeline Medallón en PySpark."""

from datetime import datetime
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
AUDIT_PATH = str(PROJECT_ROOT / "audit" / "audit_pipeline.parquet")

AUDIT_SCHEMA = StructType([
    StructField("id_ejecucion", StringType(), False),
    StructField("capa", StringType(), False),
    StructField("proceso_script", StringType(), False),
    StructField("fuente_tabla", StringType(), False),
    StructField("fecha_inicio", TimestampType(), False),
    StructField("fecha_fin", TimestampType(), False),
    StructField("duracion_seg", DoubleType(), False),
    StructField("filas_leidas", LongType(), False),
    StructField("filas_escritas", LongType(), False),
    StructField("filas_descartadas", LongType(), False),
    StructField("estado", StringType(), False),
    StructField("observaciones", StringType(), True),
])


def registrar_auditoria(
    spark: SparkSession,
    id_ejecucion: str,
    capa: str,
    proceso_script: str,
    fuente_tabla: str,
    fecha_inicio: datetime,
    fecha_fin: datetime,
    filas_leidas: int,
    filas_escritas: int,
    filas_descartadas: int = 0,
    estado: str = "EXITOSO",
    observaciones: str = "",
    audit_path: str = AUDIT_PATH,
) -> None:
    """Registra una fila de auditoría en modo append sobre audit_pipeline.parquet."""
    duracion_seg = round((fecha_fin - fecha_inicio).total_seconds(), 2)
    registro = [(
        str(id_ejecucion),
        str(capa),
        str(proceso_script),
        str(fuente_tabla),
        fecha_inicio,
        fecha_fin,
        float(duracion_seg),
        int(filas_leidas),
        int(filas_escritas),
        int(filas_descartadas),
        str(estado),
        str(observaciones or ""),
    )]
    df_audit = spark.createDataFrame(registro, schema=AUDIT_SCHEMA)
    df_audit.coalesce(1).write.mode("append").parquet(audit_path)
    print(
        f"[AUDIT] {capa} | {fuente_tabla} | leidas={filas_leidas:,} | "
        f"escritas={filas_escritas:,} | descartadas={filas_descartadas:,} | "
        f"duracion={duracion_seg:.2f}s | {estado}"
    )
