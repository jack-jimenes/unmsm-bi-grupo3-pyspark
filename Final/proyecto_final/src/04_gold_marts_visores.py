"""Datamarts Gold agregados (<5 MB totales) para consumo rápido en Streamlit y Dash sin Spark JVM."""

import os
import sys
from datetime import datetime
from pathlib import Path
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from audit_logger import registrar_auditoria

PROJECT_ROOT = Path(__file__).resolve().parent.parent
GOLD_BASE = str(PROJECT_ROOT / "gold")
MART_BASE = str(PROJECT_ROOT / "gold_marts")


def crear_spark() -> SparkSession:
    """Inicializa SparkSession replicando la configuración estándar del curso."""
    spark = (
        SparkSession.builder
        .appName("ProyectoFinal_04_Gold_Marts_Visores")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.shuffle.partitions", "50")
        .config("spark.memory.fraction", "0.8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def exportar_mart(
    spark: SparkSession,
    df: DataFrame,
    nombre: str,
    filas_leidas: int,
    id_ejecucion: str,
    observaciones: str,
) -> DataFrame:
    """Escribe un datamart Parquet compacto (coalesce(1)) y registra auditoría."""
    t_ini = datetime.now()
    ruta = f"{MART_BASE}/{nombre}"
    df.coalesce(1).write.mode("overwrite").parquet(ruta)
    df_out = spark.read.parquet(ruta)
    filas_escritas = df_out.count()
    assert filas_escritas > 0, f"{nombre} resultó vacío"
    registrar_auditoria(
        spark=spark,
        id_ejecucion=id_ejecucion,
        capa="GOLD_MARTS",
        proceso_script="04_gold_marts_visores.py",
        fuente_tabla=f"gold_marts/{nombre}",
        fecha_inicio=t_ini,
        fecha_fin=datetime.now(),
        filas_leidas=filas_leidas,
        filas_escritas=filas_escritas,
        filas_descartadas=max(0, filas_leidas - filas_escritas),
        estado="EXITOSO",
        observaciones=observaciones,
    )
    return df_out


def construir_mart_municipal_anual(spark: SparkSession, id_ejecucion: str) -> None:
    """Mart 1: Resumen anual por municipalidad + variables RRHH/TIC RENAMU (Streamlit & Dash)."""
    f_ing = spark.read.parquet(f"{GOLD_BASE}/fact_ejecucion_ingresos")
    n_in = f_ing.count()
    d_ent = spark.read.parquet(f"{GOLD_BASE}/dim_entidad_municipalidad").select(
        "SK_Municipalidad", "Nombre_Municipalidad", "Tipomuni", "Tipo_Municipalidad",
        "Clasificacion_ATM", "Estado_Censal",
        "P32_1_T_Personal_Trib", "Personal_Total",
        "Software_Tributario", "Catastro_Urbano", "Pasarela_Pagos_Web",
        "P33A", "Indice_TIC_Rentas",
    )
    d_geo = spark.read.parquet(f"{GOLD_BASE}/dim_geografia_ubigeo").select(
        "SK_Ubigeo", "Cod_Departamento", "Departamento", "Provincia", "Distrito", "Region_Natural",
    )
    d_cla = spark.read.parquet(f"{GOLD_BASE}/dim_clasificador_ingreso").select("SK_Clasificador", "ES_PREDIAL")

    df_agg = (
        f_ing
        .join(F.broadcast(d_cla), on="SK_Clasificador", how="left")
        .groupBy(F.col("ANO_DOC").alias("Anio"), "SK_Municipalidad", "SEC_EJEC", "SK_Ubigeo")
        .agg(
            F.round(F.sum("MONTO_PIA"), 2).cast(DoubleType()).alias("PIA"),
            F.round(F.sum("MONTO_PIM"), 2).cast(DoubleType()).alias("PIM"),
            F.round(F.sum("MONTO_RECAUDADO"), 2).cast(DoubleType()).alias("Recaudado"),
            F.round(
                F.sum(F.when(F.col("RUBRO").isin("08", "09"), F.col("MONTO_RECAUDADO")).otherwise(0)), 2
            ).cast(DoubleType()).alias("Ingresos_Propios"),
            F.round(
                F.sum(F.when(F.col("RUBRO").isin("07", "18"), F.col("MONTO_RECAUDADO")).otherwise(0)), 2
            ).cast(DoubleType()).alias("Transferencias"),
            F.round(
                F.sum(F.when(F.col("ES_PREDIAL"), F.col("MONTO_RECAUDADO")).otherwise(0)), 2
            ).cast(DoubleType()).alias("Recaudado_Predial"),
            F.round(
                F.sum(F.when(F.col("ES_EXTORNO_NEGATIVO"), F.col("MONTO_RECAUDADO")).otherwise(0)), 2
            ).cast(DoubleType()).alias("Monto_Extornos"),
            F.sum(F.when(F.col("ES_EXTORNO_NEGATIVO"), F.lit(1)).otherwise(F.lit(0))).alias("Conteo_Extornos"),
        )
        .join(F.broadcast(d_ent), on="SK_Municipalidad", how="left")
        .join(F.broadcast(d_geo), on="SK_Ubigeo", how="left")
        .withColumn("UBIGEO", F.col("SK_Ubigeo"))
        .select(
            "Anio", "SK_Municipalidad", "SEC_EJEC", "UBIGEO", "Nombre_Municipalidad",
            "Cod_Departamento", "Departamento", "Provincia", "Distrito", "Region_Natural",
            "Tipomuni", "Tipo_Municipalidad", "Clasificacion_ATM", "Estado_Censal",
            "P32_1_T_Personal_Trib", "Personal_Total",
            "Software_Tributario", "Catastro_Urbano", "Pasarela_Pagos_Web", "P33A",
            F.col("Indice_TIC_Rentas").cast(DoubleType()).alias("Indice_TIC_Rentas"),
            "PIA", "PIM", "Recaudado", "Ingresos_Propios", "Transferencias",
            "Recaudado_Predial", "Monto_Extornos", "Conteo_Extornos",
        )
    )
    exportar_mart(
        spark, df_agg, "mart_municipal_anual", n_in, id_ejecucion,
        "Grano Anio x Municipalidad con ejecución SIAF, rubros propios/transferencias y RRHH/TIC RENAMU",
    )


def construir_mart_depto_rubro_anual(spark: SparkSession, id_ejecucion: str) -> None:
    """Mart 2: Resumen territorial por año, departamento, tipología y rubro (Dash mapa + autonomía)."""
    f_ing = spark.read.parquet(f"{GOLD_BASE}/fact_ejecucion_ingresos")
    n_in = f_ing.count()
    d_ent = spark.read.parquet(f"{GOLD_BASE}/dim_entidad_municipalidad").select(
        "SK_Municipalidad", "Tipo_Municipalidad",
    )
    d_geo = spark.read.parquet(f"{GOLD_BASE}/dim_geografia_ubigeo").select(
        "SK_Ubigeo", "Cod_Departamento", "Departamento", "Region_Natural",
    )
    d_rub = spark.read.parquet(f"{GOLD_BASE}/dim_fuente_rubro").select(
        "SK_Fuente_Rubro", "RUBRO_NOMBRE", "FUENTE_FINANCIAMIENTO_NOMBRE",
    )

    df_agg = (
        f_ing
        .join(F.broadcast(d_ent), on="SK_Municipalidad", how="left")
        .join(F.broadcast(d_geo), on="SK_Ubigeo", how="left")
        .join(F.broadcast(d_rub), on="SK_Fuente_Rubro", how="left")
        .groupBy(
            F.col("ANO_DOC").alias("Anio"),
            "Cod_Departamento", "Departamento", "Region_Natural", "Tipo_Municipalidad",
            "RUBRO", "RUBRO_NOMBRE", "FUENTE_FINANCIAMIENTO_NOMBRE",
        )
        .agg(
            F.round(F.sum("MONTO_PIA"), 2).cast(DoubleType()).alias("PIA"),
            F.round(F.sum("MONTO_PIM"), 2).cast(DoubleType()).alias("PIM"),
            F.round(F.sum("MONTO_RECAUDADO"), 2).cast(DoubleType()).alias("Recaudado"),
            F.countDistinct("SK_Municipalidad").alias("Num_Municipalidades"),
        )
    )
    exportar_mart(
        spark, df_agg, "mart_depto_rubro_anual", n_in, id_ejecucion,
        "Grano Anio x Cod_Departamento x Tipo_Municipalidad x Rubro para mapa coroplético Dash",
    )


def construir_mart_atm_tramos_anual(spark: SparkSession, id_ejecucion: str) -> None:
    """Mart 3: Desempeño predial anual (MES=13) por Tramo ATM, Departamento y Tipología."""
    f_meta = spark.read.parquet(f"{GOLD_BASE}/fact_metas_estadistica_atm").filter(F.col("MES_ESTADISTICA") == 13)
    n_in = f_meta.count()
    d_ent = spark.read.parquet(f"{GOLD_BASE}/dim_entidad_municipalidad").select(
        "SK_Municipalidad", "Tipo_Municipalidad", "Clasificacion_ATM",
    )
    d_geo = spark.read.parquet(f"{GOLD_BASE}/dim_geografia_ubigeo").select(
        "SK_Ubigeo", "Cod_Departamento", "Departamento", "Region_Natural",
    )

    df_agg = (
        f_meta
        .join(F.broadcast(d_ent), on="SK_Municipalidad", how="left")
        .join(F.broadcast(d_geo), on="SK_Ubigeo", how="left")
        .groupBy(
            F.col("ANO_ESTADISTICA").alias("Anio"),
            F.coalesce(F.col("Clasificacion_ATM"), F.lit("Sin Tramo")).alias("Clasificacion_ATM"),
            "Cod_Departamento", "Departamento", "Region_Natural", "Tipo_Municipalidad",
        )
        .agg(
            F.round(
                F.sum(F.when((F.col("VIG_EMISION") == 1) & (F.col("Ind_Outlier_Emision") == 0), F.col("Monto_Emision_Predial")).otherwise(0)),
                2,
            ).cast(DoubleType()).alias("Emision_Predial"),
            F.round(
                F.sum(F.when(F.col("VIG_RECAUD") == 1, F.col("Monto_Recaudado_Ord")).otherwise(0)), 2
            ).cast(DoubleType()).alias("Recaudado_Ord"),
            F.round(
                F.sum(F.when(F.col("VIG_RECAUD") == 1, F.col("Monto_Recaudado_Coact")).otherwise(0)), 2
            ).cast(DoubleType()).alias("Recaudado_Coact"),
            F.round(
                F.sum(F.when((F.col("VIG_SALDO") == 1) & (F.col("Ind_Outlier_Saldo") == 0), F.col("Monto_Saldo_Ord")).otherwise(0)),
                2,
            ).cast(DoubleType()).alias("Saldo_Ord_Real"),
            F.round(
                F.sum(F.when((F.col("VIG_SALDO") == 1) & (F.col("Ind_Outlier_Saldo") == 0), F.col("Monto_Saldo_Coact")).otherwise(0)),
                2,
            ).cast(DoubleType()).alias("Saldo_Coact_Real"),
            F.countDistinct(F.when(F.col("Ind_Cumplimiento_Meta") == 1, F.col("SEC_EJEC"))).alias("Munis_Cumplen_Meta"),
            F.countDistinct(F.when(F.col("Ind_Cumplimiento_Meta").isNotNull(), F.col("SEC_EJEC"))).alias("Munis_Evaluadas"),
        )
    )
    exportar_mart(
        spark, df_agg, "mart_atm_tramos_anual", n_in, id_ejecucion,
        "Cierre anual Predial (MES=13) sin atípicos agrupado por Clasificacion_ATM y departamento",
    )


def construir_mart_madurez_coactiva(spark: SparkSession, id_ejecucion: str) -> None:
    """Mart 4: Madurez institucional ATM (Formularios 2A/2B/2C) x TIC/Coactivo x Predial."""
    f_cuest = spark.read.parquet(f"{GOLD_BASE}/fact_cuestionario_atm")
    n_in = f_cuest.count()
    f_meta_13 = (
        spark.read.parquet(f"{GOLD_BASE}/fact_metas_estadistica_atm")
        .filter(F.col("MES_ESTADISTICA") == 13)
        .select(
            F.col("SEC_EJEC"),
            F.col("ANO_ESTADISTICA").alias("ANO_APLICACION"),
            F.when((F.col("VIG_EMISION") == 1) & (F.col("Ind_Outlier_Emision") == 0), F.col("Monto_Emision_Predial")).otherwise(0).alias("_em"),
            F.when(F.col("VIG_RECAUD") == 1, F.col("Monto_Recaudado_Ord")).otherwise(0).alias("_ro"),
            F.when(F.col("VIG_RECAUD") == 1, F.col("Monto_Recaudado_Coact")).otherwise(0).alias("_rc"),
            F.when((F.col("VIG_SALDO") == 1) & (F.col("Ind_Outlier_Saldo") == 0), F.col("Monto_Saldo_Ord")).otherwise(0).alias("_so"),
            F.when((F.col("VIG_SALDO") == 1) & (F.col("Ind_Outlier_Saldo") == 0), F.col("Monto_Saldo_Coact")).otherwise(0).alias("_sc"),
        )
        .groupBy("SEC_EJEC", "ANO_APLICACION")
        .agg(
            F.round(F.sum("_em"), 2).cast(DoubleType()).alias("Monto_Emision_Predial"),
            F.round(F.sum("_ro"), 2).cast(DoubleType()).alias("Monto_Recaudado_Ord"),
            F.round(F.sum("_rc"), 2).cast(DoubleType()).alias("Monto_Recaudado_Coact"),
            F.round(F.sum("_so"), 2).cast(DoubleType()).alias("Monto_Saldo_Ord"),
            F.round(F.sum("_sc"), 2).cast(DoubleType()).alias("Monto_Saldo_Coact"),
        )
    )
    d_ent = spark.read.parquet(f"{GOLD_BASE}/dim_entidad_municipalidad").select(
        "SEC_EJEC", "SK_Ubigeo", "Nombre_Municipalidad", "Tipo_Municipalidad",
        F.coalesce(F.col("Clasificacion_ATM"), F.lit("Sin Tramo")).alias("Clasificacion_ATM"),
        "P33A", "Software_Tributario", "Catastro_Urbano", "Pasarela_Pagos_Web",
        F.col("Indice_TIC_Rentas").cast(DoubleType()).alias("Indice_TIC_Rentas"),
        "P32_1_T_Personal_Trib", "Personal_Total",
    )
    d_geo = spark.read.parquet(f"{GOLD_BASE}/dim_geografia_ubigeo").select(
        "SK_Ubigeo", "Cod_Departamento", "Departamento", "Region_Natural",
    )

    df_mad = (
        f_cuest
        .groupBy("ANO_APLICACION", "SEC_EJEC")
        .agg(
            F.round(F.max("Puntaje_Madurez_ATM"), 2).cast(DoubleType()).alias("Puntaje_Madurez_ATM"),
            F.max("Tiene_Coactivo_Activo").alias("Tiene_Coactivo_Activo"),
            F.max("Tiene_Sistema_Rentas").alias("Tiene_Sistema_Rentas"),
        )
        .join(F.broadcast(d_ent), on="SEC_EJEC", how="left")
        .join(F.broadcast(d_geo), on="SK_Ubigeo", how="left")
        .join(f_meta_13, on=["SEC_EJEC", "ANO_APLICACION"], how="left")
        .fillna(0.0, subset=[
            "Monto_Emision_Predial", "Monto_Recaudado_Ord",
            "Monto_Recaudado_Coact", "Monto_Saldo_Ord", "Monto_Saldo_Coact",
        ])
        .select(
            "ANO_APLICACION", "SEC_EJEC", "Nombre_Municipalidad",
            "Cod_Departamento", "Departamento", "Region_Natural",
            "Tipo_Municipalidad", "Clasificacion_ATM",
            "Puntaje_Madurez_ATM", "Tiene_Coactivo_Activo", "Tiene_Sistema_Rentas",
            "P33A", "Software_Tributario", "Catastro_Urbano", "Pasarela_Pagos_Web",
            "Indice_TIC_Rentas", "P32_1_T_Personal_Trib", "Personal_Total",
            "Monto_Emision_Predial", "Monto_Recaudado_Ord", "Monto_Recaudado_Coact",
            "Monto_Saldo_Ord", "Monto_Saldo_Coact",
        )
    )
    exportar_mart(
        spark, df_mad, "mart_madurez_coactiva", n_in, id_ejecucion,
        "Madurez ATM (2A/2B/2C) + TIC/Coactivo RENAMU + Recaudación/Saldo Predial por campaña",
    )


def main() -> None:
    os.makedirs(MART_BASE, exist_ok=True)
    id_ejecucion = f"MARTS_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:6]}"
    spark = crear_spark()
    try:
        construir_mart_municipal_anual(spark, id_ejecucion)
        construir_mart_depto_rubro_anual(spark, id_ejecucion)
        construir_mart_atm_tramos_anual(spark, id_ejecucion)
        construir_mart_madurez_coactiva(spark, id_ejecucion)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
