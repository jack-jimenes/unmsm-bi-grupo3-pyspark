"""Capa Silver: Limpieza, estandarización y reglas de calidad para SIAF, SISMEPRE y RENAMU."""

import os
import sys
from datetime import datetime
from pathlib import Path
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ByteType, DecimalType, IntegerType, LongType, ShortType

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from audit_logger import registrar_auditoria

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BRONZE_BASE = str(PROJECT_ROOT / "bronze")
SILVER_BASE = str(PROJECT_ROOT / "silver")


def crear_spark() -> SparkSession:
    """Inicializa SparkSession replicando la configuración estándar del curso."""
    spark = (
        SparkSession.builder
        .appName("ProyectoFinal_02_Silver_Limpieza")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.shuffle.partitions", "50")
        .config("spark.memory.fraction", "0.8")
        .getOrCreate()
    )
    spark.sparkContext.setCheckpointDir("/tmp/spark_checkpoint")
    spark.sparkContext.setLogLevel("WARN")
    return spark


def limpiar_cols_texto(df: DataFrame, excluir: set[str] | None = None) -> DataFrame:
    """Aplica TRIM, colapsa espacios múltiples internos y convierte '' en NULL."""
    excluir = excluir or set()
    for c in df.columns:
        if c in excluir:
            continue
        limpio = F.regexp_replace(F.trim(F.col(c)), r"\s+", " ")
        df = df.withColumn(c, F.when(limpio == F.lit(""), F.lit(None)).otherwise(limpio))
    return df


def parsear_fecha(col_name: str) -> F.Column:
    """Convierte '0' a NULL y parsea formatos de fecha SISMEPRE (d/M/yyyy HH:mm:ss o d/M/yyyy)."""
    c = F.col(col_name)
    c_clean = F.when(c.isNull() | (c == F.lit("0")) | (F.trim(c) == F.lit("")), F.lit(None)).otherwise(F.trim(c))
    return F.coalesce(
        F.to_date(c_clean, "d/M/yyyy HH:mm:ss"),
        F.to_date(c_clean, "d/M/yyyy"),
        F.to_date(c_clean, "yyyy-MM-dd HH:mm:ss"),
        F.to_date(c_clean, "yyyy-MM-dd"),
    )


def limpiar_siaf(spark: SparkSession, id_ejecucion: str) -> None:
    """Sanea los 18 archivos netos del SIAF filtrando estrictamente Municipalidades ('M')."""
    t_ini = datetime.now()
    df_bronze = spark.read.parquet(f"{BRONZE_BASE}/siaf")
    filas_leidas = df_bronze.count()

    # Regla 1: Excluir cortes superpuestos 2025/2026-Ingreso-Mensual.csv
    # Regla Sesión 13 (Prof. Lezama): Segmentar Silver estrictamente a Municipalidades (NIVEL_GOBIERNO = 'M')
    df_neto = (
        df_bronze
        .filter(~F.col("_archivo_origen").rlike(r"2025-Ingreso-Mensual\.csv|2026-Ingreso-Mensual\.csv"))
        .filter(F.trim(F.col("NIVEL_GOBIERNO")) == F.lit("M"))
    )

    # Reglas 10 y 12: TRIM, colapso de espacios múltiples y '' -> NULL
    no_texto = {"MONTO_PIA", "MONTO_PIM", "MONTO_RECAUDADO", "_archivo_origen"}
    df_clean = limpiar_cols_texto(df_neto, excluir=no_texto)

    # Regla 11: Normalización de códigos con LPAD, '0' -> NULL en departamento y UBIGEO de 6 dígitos
    dep_norm = (
        F.when(
            F.col("DEPARTAMENTO_EJECUTORA").isNull() | F.col("DEPARTAMENTO_EJECUTORA").isin("0", "00"),
            F.lit(None),
        )
        .otherwise(F.lpad(F.col("DEPARTAMENTO_EJECUTORA"), 2, "0"))
    )
    prov_norm = F.when(
        F.col("PROVINCIA_EJECUTORA").isNotNull(),
        F.lpad(F.col("PROVINCIA_EJECUTORA"), 2, "0"),
    )
    dist_norm = F.when(
        F.col("DISTRITO_EJECUTORA").isNotNull(),
        F.lpad(F.col("DISTRITO_EJECUTORA"), 2, "0"),
    )

    df_silver = (
        df_clean
        .withColumn("ANO_DOC", F.col("ANO_DOC").cast(ShortType()))
        .withColumn("MES_DOC", F.col("MES_DOC").cast(ByteType()))
        .withColumn("FECHA", F.make_date(F.col("ANO_DOC").cast(IntegerType()), F.col("MES_DOC").cast(IntegerType()), F.lit(1)))
        .withColumn("SEC_EJEC", F.col("SEC_EJEC").cast(IntegerType()))
        .withColumn("SECTOR", F.when(F.col("SECTOR").isNotNull(), F.lpad(F.col("SECTOR"), 2, "0")))
        .withColumn("PLIEGO", F.when(F.col("PLIEGO").isNotNull(), F.lpad(F.col("PLIEGO"), 3, "0")))
        .withColumn("DEPARTAMENTO_EJECUTORA", dep_norm)
        .withColumn("PROVINCIA_EJECUTORA", prov_norm)
        .withColumn("DISTRITO_EJECUTORA", dist_norm)
        .withColumn(
            "UBIGEO",
            F.when(
                F.col("DEPARTAMENTO_EJECUTORA").isNotNull()
                & F.col("PROVINCIA_EJECUTORA").isNotNull()
                & F.col("DISTRITO_EJECUTORA").isNotNull(),
                F.concat(F.col("DEPARTAMENTO_EJECUTORA"), F.col("PROVINCIA_EJECUTORA"), F.col("DISTRITO_EJECUTORA")),
            ).otherwise(F.lit(None)),
        )
        .withColumn("RUBRO", F.lpad(F.col("RUBRO"), 2, "0"))
        .withColumn("TIPO_RECURSO_KEY", F.concat_ws(".", F.col("RUBRO"), F.col("TIPO_RECURSO")))
        .withColumn(
            "CLASIFICADOR",
            F.concat_ws(
                ".",
                F.lit("1"),
                F.col("GENERICA"),
                F.col("SUBGENERICA"),
                F.col("SUBGENERICA_DET"),
                F.col("ESPECIFICA"),
                F.col("ESPECIFICA_DET"),
            ),
        )
        .withColumn("ES_PREDIAL", F.col("CLASIFICADOR").startswith("1.1.2.1.1"))
        .withColumn("MONTO_PIA", F.trim(F.col("MONTO_PIA")).cast(DecimalType(18, 2)))
        .withColumn("MONTO_PIM", F.trim(F.col("MONTO_PIM")).cast(DecimalType(18, 2)))
        .withColumn("MONTO_RECAUDADO", F.trim(F.col("MONTO_RECAUDADO")).cast(DecimalType(18, 2)))
        .withColumn("ES_EXTORNO_NEGATIVO", F.col("MONTO_RECAUDADO") < F.lit(0))
        .withColumn(
            "ES_FILA_SIN_MONTO",
            (F.col("MONTO_PIA") == F.lit(0))
            & (F.col("MONTO_PIM") == F.lit(0))
            & (F.col("MONTO_RECAUDADO") == F.lit(0)),
        )
    )

    ruta_dst = f"{SILVER_BASE}/siaf"
    df_silver.write.mode("overwrite").parquet(ruta_dst)

    filas_escritas = spark.read.parquet(ruta_dst).count()
    filas_descartadas = filas_leidas - filas_escritas
    t_fin = datetime.now()

    assert filas_escritas == 12735873, (
        f"SIAF Silver Municipal esperaba 12,735,873 filas, obtuvo {filas_escritas:,}"
    )
    registrar_auditoria(
        spark=spark,
        id_ejecucion=id_ejecucion,
        capa="SILVER",
        proceso_script="02_silver_limpieza.py",
        fuente_tabla="silver/siaf",
        fecha_inicio=t_ini,
        fecha_fin=t_fin,
        filas_leidas=filas_leidas,
        filas_escritas=filas_escritas,
        filas_descartadas=filas_descartadas,
        estado="EXITOSO",
        observaciones="Filtro estricto NIVEL_GOBIERNO='M' + exclusión 2025/2026-Mensual (-3,983,680 filas, -23.83% vs Bronze Bruto); UBIGEO LPAD 6 dígitos; 72,194 extornos negativos preservados",
    )


def limpiar_sismepre_tablas_maestras(spark: SparkSession, id_ejecucion: str) -> None:
    """Limpia y tipa las 6 tablas auxiliares y de respuestas de SISMEPRE."""
    # 1. rentas_ano_aplicacion
    t_ini = datetime.now()
    df_ano = spark.read.parquet(f"{BRONZE_BASE}/sismepre/rentas_ano_aplicacion")
    n_in = df_ano.count()
    df_ano = (
        limpiar_cols_texto(df_ano, {"_archivo_origen"})
        .withColumn("ANO_APLICACION", F.col("ANO_APLICACION").cast(IntegerType()))
        .withColumn("ANO_APLICACION_INICIO", F.col("ANO_APLICACION_INICIO").cast(IntegerType()))
        .withColumn("ANO_APLICACION_FIN", F.col("ANO_APLICACION_FIN").cast(IntegerType()))
        .withColumn("PERIODO", F.col("PERIODO").cast(IntegerType()))
        .withColumn("FECHA_CIERRE", parsear_fecha("FECHA_CIERRE"))
        .withColumn("FECHA_PRES_OFICIO", parsear_fecha("FECHA_PRES_OFICIO"))
        .withColumn("FECHA_INI_CIERRE", parsear_fecha("FECHA_INI_CIERRE"))
        .withColumn("FECHA_ING", parsear_fecha("FECHA_ING"))
    )
    df_ano.write.mode("overwrite").parquet(f"{SILVER_BASE}/sismepre/rentas_ano_aplicacion")
    n_out = spark.read.parquet(f"{SILVER_BASE}/sismepre/rentas_ano_aplicacion").count()
    registrar_auditoria(
        spark, id_ejecucion, "SILVER", "02_silver_limpieza.py",
        "silver/sismepre/rentas_ano_aplicacion", t_ini, datetime.now(), n_in, n_out, n_in - n_out,
        observaciones="Maestro de campañas tipado y con espacios vacíos convertidos a NULL",
    )

    # 2. rentas_entidad_estado
    t_ini = datetime.now()
    df_est = spark.read.parquet(f"{BRONZE_BASE}/sismepre/rentas_entidad_estado")
    n_in = df_est.count()
    df_est = (
        limpiar_cols_texto(df_est, {"_archivo_origen"})
        .drop("ESTADO", "ORIGEN_INFORMACION", "USUARIO_ENVIO_ID", "USUARIO_FECHA_ENVIO", "CORREO")
        .withColumn("SEC_EJEC", F.col("SEC_EJEC").cast(IntegerType()))
        .withColumn("ANO_APLICACION", F.col("ANO_APLICACION").cast(IntegerType()))
        .withColumn("PERIODO", F.col("PERIODO").cast(IntegerType()))
        .withColumn("TIPO_META", F.col("TIPO_META").cast(IntegerType()))
        .withColumn("USUARIO_CREACION_FECHA", parsear_fecha("USUARIO_CREACION_FECHA"))
        .withColumn("FECHA_RESOL_ALCAL_ADJUNTO", parsear_fecha("FECHA_RESOL_ALCAL_ADJUNTO"))
    )
    df_est.write.mode("overwrite").parquet(f"{SILVER_BASE}/sismepre/rentas_entidad_estado")
    n_out = spark.read.parquet(f"{SILVER_BASE}/sismepre/rentas_entidad_estado").count()
    registrar_auditoria(
        spark, id_ejecucion, "SILVER", "02_silver_limpieza.py",
        "silver/sismepre/rentas_entidad_estado", t_ini, datetime.now(), n_in, n_out, n_in - n_out,
        observaciones="Columnas degeneradas eliminadas y '0' -> NULL en FECHA_RESOL_ALCAL_ADJUNTO",
    )

    # 3. rentas_formulario
    t_ini = datetime.now()
    df_form = spark.read.parquet(f"{BRONZE_BASE}/sismepre/rentas_formulario")
    n_in = df_form.count()
    df_form = (
        limpiar_cols_texto(df_form, {"_archivo_origen"})
        .drop("ESTADO_REGISTRO")
        .withColumn("ANO_APLICACION", F.col("ANO_APLICACION").cast(IntegerType()))
        .withColumn("PERIODO", F.col("PERIODO").cast(IntegerType()))
        .withColumn("FORMULARIO_ID", F.col("FORMULARIO_ID").cast(IntegerType()))
        .withColumn("ES_ARBITRIOS", F.upper(F.col("TITULO")).contains("ARBITRIO"))
    )
    df_form.write.mode("overwrite").parquet(f"{SILVER_BASE}/sismepre/rentas_formulario")
    n_out = spark.read.parquet(f"{SILVER_BASE}/sismepre/rentas_formulario").count()
    registrar_auditoria(
        spark, id_ejecucion, "SILVER", "02_silver_limpieza.py",
        "silver/sismepre/rentas_formulario", t_ini, datetime.now(), n_in, n_out, n_in - n_out,
        observaciones="Flag ES_ARBITRIOS por TITULO y eliminación de ESTADO_REGISTRO",
    )

    # 4. rentas_estadistica
    t_ini = datetime.now()
    df_ecat = spark.read.parquet(f"{BRONZE_BASE}/sismepre/rentas_estadistica")
    n_in = df_ecat.count()
    df_ecat = (
        limpiar_cols_texto(df_ecat, {"_archivo_origen"})
        .drop("ESTADO_REGISTRO")
        .withColumn("ANO_APLICACION", F.col("ANO_APLICACION").cast(IntegerType()))
        .withColumn("PERIODO", F.col("PERIODO").cast(IntegerType()))
        .withColumn("FORMULARIO_ID", F.col("FORMULARIO_ID").cast(IntegerType()))
        .withColumn("ANO_ESTADISTICA", F.col("ANO_ESTADISTICA").cast(IntegerType()))
        .withColumn("MES_ESTADISTICA", F.col("MES_ESTADISTICA").cast(IntegerType()))
    )
    df_ecat.write.mode("overwrite").parquet(f"{SILVER_BASE}/sismepre/rentas_estadistica")
    n_out = spark.read.parquet(f"{SILVER_BASE}/sismepre/rentas_estadistica").count()
    registrar_auditoria(
        spark, id_ejecucion, "SILVER", "02_silver_limpieza.py",
        "silver/sismepre/rentas_estadistica", t_ini, datetime.now(), n_in, n_out, n_in - n_out,
        observaciones="Catálogo estadístico tipado y ESTADO_REGISTRO eliminado",
    )

    # 5. rentas_preguntas
    t_ini = datetime.now()
    df_preg = spark.read.parquet(f"{BRONZE_BASE}/sismepre/rentas_preguntas")
    n_in = df_preg.count()
    df_preg = (
        limpiar_cols_texto(df_preg, {"_archivo_origen"})
        .drop("ESTADO_REGISTRO")
        .withColumn("ANO_APLICACION", F.col("ANO_APLICACION").cast(IntegerType()))
        .withColumn("PERIODO", F.col("PERIODO").cast(IntegerType()))
        .withColumn("FORMULARIO_ID", F.col("FORMULARIO_ID").cast(IntegerType()))
        .withColumn("PREGUNTA_ID", F.col("PREGUNTA_ID").cast(IntegerType()))
        .withColumn("PREGUNTA_PADRE_ID", F.col("PREGUNTA_PADRE_ID").cast(IntegerType()))
        .withColumn("TIPO_CUESTIONARIO_ID", F.col("TIPO_CUESTIONARIO_ID").cast(IntegerType()))
    )
    df_preg.write.mode("overwrite").parquet(f"{SILVER_BASE}/sismepre/rentas_preguntas")
    n_out = spark.read.parquet(f"{SILVER_BASE}/sismepre/rentas_preguntas").count()
    registrar_auditoria(
        spark, id_ejecucion, "SILVER", "02_silver_limpieza.py",
        "silver/sismepre/rentas_preguntas", t_ini, datetime.now(), n_in, n_out, n_in - n_out,
        observaciones="Catálogo de 836 preguntas tipado y sin ESTADO_REGISTRO",
    )

    # 6. rentas_respuestas
    t_ini = datetime.now()
    df_resp = spark.read.parquet(f"{BRONZE_BASE}/sismepre/rentas_respuestas")
    n_in = df_resp.count()
    df_resp = (
        limpiar_cols_texto(df_resp, {"_archivo_origen"})
        .drop("ESTADO_REGISTRO")
        .withColumn("SEC_EJEC", F.col("SEC_EJEC").cast(IntegerType()))
        .withColumn("ANO_APLICACION", F.col("ANO_APLICACION").cast(IntegerType()))
        .withColumn("PERIODO", F.col("PERIODO").cast(IntegerType()))
        .withColumn("FORMULARIO_ID", F.col("FORMULARIO_ID").cast(IntegerType()))
        .withColumn("PREGUNTA_ID", F.col("PREGUNTA_ID").cast(IntegerType()))
        .withColumn("RESPUESTA_ID", F.col("RESPUESTA_ID").cast(IntegerType()))
        .withColumn("RESPUESTA_DECIMAL", F.coalesce(F.col("RESPUESTA_DECIMAL").cast(DecimalType(18, 2)), F.lit(0).cast(DecimalType(18, 2))))
        .withColumn("RESPUESTA_ENTERO", F.coalesce(F.col("RESPUESTA_ENTERO").cast(LongType()), F.lit(0).cast(LongType())))
        .withColumn("RESPUESTA_FECHA", parsear_fecha("RESPUESTA_FECHA"))
    )
    df_resp.write.mode("overwrite").parquet(f"{SILVER_BASE}/sismepre/rentas_respuestas")
    n_out = spark.read.parquet(f"{SILVER_BASE}/sismepre/rentas_respuestas").count()
    assert n_out == 322675, f"rentas_respuestas Silver esperaba 322,675 filas, obtuvo {n_out:,}"
    registrar_auditoria(
        spark, id_ejecucion, "SILVER", "02_silver_limpieza.py",
        "silver/sismepre/rentas_respuestas", t_ini, datetime.now(), n_in, n_out, n_in - n_out,
        observaciones="322,675 respuestas limpias, tipadas y fechas saneadas",
    )


def limpiar_esat(spark: SparkSession, id_ejecucion: str) -> None:
    """Limpia rentas_esat_estadistica_atm: excluye Arbitrios por TITULO, vigencia por bloque, cierre anual y outliers."""
    t_ini = datetime.now()
    df_raw = spark.read.parquet(f"{BRONZE_BASE}/sismepre/rentas_esat_estadistica_atm")
    filas_leidas = df_raw.count()

    df_form = (
        spark.read.parquet(f"{SILVER_BASE}/sismepre/rentas_formulario")
        .select("ANO_APLICACION", "PERIODO", "FORMULARIO_ID", "TITULO", "ES_ARBITRIOS")
    )

    df_clean = limpiar_cols_texto(df_raw, {"_archivo_origen"}).drop("TIPO_META")

    mon_cols = [c for c in df_clean.columns if c.startswith("MON_")]
    num_cols = [c for c in df_clean.columns if c.startswith("NUM_")]

    df_typed = (
        df_clean
        .withColumn("SEC_EJEC", F.col("SEC_EJEC").cast(IntegerType()))
        .withColumn("ANO_APLICACION", F.col("ANO_APLICACION").cast(IntegerType()))
        .withColumn("PERIODO", F.col("PERIODO").cast(IntegerType()))
        .withColumn("FORMULARIO_ID", F.col("FORMULARIO_ID").cast(IntegerType()))
        .withColumn("ANO_ESTADISTICA", F.col("ANO_ESTADISTICA").cast(IntegerType()))
        .withColumn("MES_ESTADISTICA", F.col("MES_ESTADISTICA").cast(IntegerType()))
        .withColumn("UBIGEO", F.lpad(F.col("UBIGEO"), 6, "0"))
        .withColumn("DEPARTAMENTO", F.lpad(F.col("DEPARTAMENTO"), 2, "0"))
        .withColumn("PROVINCIA", F.lpad(F.col("PROVINCIA"), 2, "0"))
        .withColumn("DISTRITO", F.lpad(F.col("DISTRITO"), 2, "0"))
    )

    for c in mon_cols:
        df_typed = df_typed.withColumn(
            c, F.coalesce(F.col(c).cast(DecimalType(18, 2)), F.lit(0).cast(DecimalType(18, 2)))
        )
    for c in num_cols:
        df_typed = df_typed.withColumn(
            c, F.coalesce(F.col(c).cast(LongType()), F.lit(0).cast(LongType()))
        )

    # Excluir formularios de Arbitrios identificándolos exclusivamente por TITULO (nunca por FORMULARIO_ID)
    df_predial = (
        df_typed
        .join(F.broadcast(df_form), on=["ANO_APLICACION", "PERIODO", "FORMULARIO_ID"], how="left")
        .filter(F.col("TITULO").isNull() | (~F.upper(F.col("TITULO")).contains("ARBITRIO")))
        .drop("ES_ARBITRIOS")
    )

    filas_predial = df_predial.count()
    filas_arbitrios_desc = filas_leidas - filas_predial
    assert filas_predial == 110549, f"ESAT Predial esperaba 110,549 filas, obtuvo {filas_predial:,}"

    # Banderas de dato real por bloque
    has_emi = F.col("MON_EMISIONPREDIAL_AFECTO") != F.lit(0)
    has_rec = (
        (F.col("MON_RECAUDACTUAL_ORDIN") != F.lit(0))
        | (F.col("MON_RECAUDACTUAL_COAC") != F.lit(0))
        | (F.col("MON_RECAUDANTER_ORDI") != F.lit(0))
        | (F.col("MON_RECAUDANTER_COAC") != F.lit(0))
    )
    has_sal = (F.col("MON_SALDOPREDIAL_ORD") != F.lit(0)) | (F.col("MON_SALDOPREDIAL_COAC") != F.lit(0))

    order_desc = [F.col("ANO_APLICACION").desc(), F.col("PERIODO").desc(), F.col("FORMULARIO_ID").desc()]
    w_emi = Window.partitionBy("SEC_EJEC", "ANO_ESTADISTICA", "MES_ESTADISTICA", has_emi).orderBy(*order_desc)
    w_rec = Window.partitionBy("SEC_EJEC", "ANO_ESTADISTICA", "MES_ESTADISTICA", has_rec).orderBy(*order_desc)
    w_sal = Window.partitionBy("SEC_EJEC", "ANO_ESTADISTICA", "MES_ESTADISTICA", has_sal).orderBy(*order_desc)

    df_vig = (
        df_predial
        .withColumn("VIG_EMISION", F.when(has_emi & (F.row_number().over(w_emi) == 1), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("VIG_RECAUD", F.when(has_rec & (F.row_number().over(w_rec) == 1), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("VIG_SALDO", F.when(has_sal & (F.row_number().over(w_sal) == 1), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("Ind_Cierre_Reconstruido", F.lit(0))
    ).checkpoint()

    # Cierre anual reconstruido: para pares (SEC_EJEC, ANO_ESTADISTICA) sin MES=13 vigente pero con los 12 meses 1..12 vigentes
    df_rec_vig = df_vig.filter(F.col("VIG_RECAUD") == 1)
    pares_con_13 = df_rec_vig.filter(F.col("MES_ESTADISTICA") == 13).select("SEC_EJEC", "ANO_ESTADISTICA").distinct()

    pares_12_meses = (
        df_rec_vig
        .filter(F.col("MES_ESTADISTICA").between(1, 12))
        .groupBy("SEC_EJEC", "ANO_ESTADISTICA")
        .agg(F.countDistinct("MES_ESTADISTICA").alias("n_meses"))
        .filter(F.col("n_meses") == 12)
        .select("SEC_EJEC", "ANO_ESTADISTICA")
        .join(pares_con_13, on=["SEC_EJEC", "ANO_ESTADISTICA"], how="left_anti")
    )

    rec_cols = {
        "MON_RECAUDACTUAL_ORDIN",
        "MON_RECAUDACTUAL_COAC",
        "MON_RECAUDANTER_ORDI",
        "MON_RECAUDANTER_COAC",
    }
    agg_exprs = [
        F.max("ANO_APLICACION").alias("ANO_APLICACION"),
        F.max("PERIODO").alias("PERIODO"),
        F.max("FORMULARIO_ID").alias("FORMULARIO_ID"),
        F.first("UBIGEO", ignorenulls=True).alias("UBIGEO"),
        F.first("DEPARTAMENTO", ignorenulls=True).alias("DEPARTAMENTO"),
        F.first("DEPARTAMENTO_NOMBRE", ignorenulls=True).alias("DEPARTAMENTO_NOMBRE"),
        F.first("PROVINCIA", ignorenulls=True).alias("PROVINCIA"),
        F.first("PROVINCIA_NOMBRE", ignorenulls=True).alias("PROVINCIA_NOMBRE"),
        F.first("DISTRITO", ignorenulls=True).alias("DISTRITO"),
        F.first("DISTRITO_NOMBRE", ignorenulls=True).alias("DISTRITO_NOMBRE"),
        F.first("MUNICIPALIDAD_NOMBRE", ignorenulls=True).alias("MUNICIPALIDAD_NOMBRE"),
        F.first("_archivo_origen", ignorenulls=True).alias("_archivo_origen"),
        F.lit(13).cast(IntegerType()).alias("MES_ESTADISTICA"),
        F.lit("CIERRE ANUAL RECONSTRUIDO (SUMA M1-M12)").alias("TITULO"),
        F.lit(0).alias("VIG_EMISION"),
        F.lit(1).alias("VIG_RECAUD"),
        F.lit(0).alias("VIG_SALDO"),
        F.lit(1).alias("Ind_Cierre_Reconstruido"),
    ]
    for c in mon_cols:
        if c in rec_cols:
            agg_exprs.append(F.sum(c).cast(DecimalType(18, 2)).alias(c))
        else:
            agg_exprs.append(F.lit(0).cast(DecimalType(18, 2)).alias(c))
    for c in num_cols:
        agg_exprs.append(F.lit(0).cast(LongType()).alias(c))

    df_cierres_rec = (
        df_rec_vig
        .filter(F.col("MES_ESTADISTICA").between(1, 12))
        .join(pares_12_meses, on=["SEC_EJEC", "ANO_ESTADISTICA"], how="inner")
        .groupBy("SEC_EJEC", "ANO_ESTADISTICA")
        .agg(*agg_exprs)
    )

    df_consolidado = df_vig.unionByName(df_cierres_rec, allowMissingColumns=True)

    # Reglas de outliers (Sección 2.2):
    # 1. Ind_Outlier_Saldo = 1 si saldo ordinario vigente > S/ 1,000 M
    # 2. Ind_Outlier_Emision = 1 si emisión anual >= 50x mediana propia y > S/ 1 M
    df_med_emi = (
        df_consolidado
        .filter((F.col("MES_ESTADISTICA") == 13) & (F.col("VIG_EMISION") == 1))
        .groupBy("SEC_EJEC")
        .agg(F.sort_array(F.collect_list("MON_EMISIONPREDIAL_AFECTO")).alias("vals"))
        .withColumn("n", F.size("vals"))
        .withColumn(
            "mediana_emision_muni",
            F.when(F.col("n") == 0, F.lit(0.0))
            .when(F.col("n") % 2 == 1, F.element_at("vals", (F.col("n") / 2 + 1).cast("int")).cast("double"))
            .otherwise(
                (
                    F.element_at("vals", (F.col("n") / 2).cast("int")).cast("double")
                    + F.element_at("vals", (F.col("n") / 2 + 1).cast("int")).cast("double")
                ) / 2.0
            ),
        )
        .select("SEC_EJEC", "mediana_emision_muni")
    )

    df_final = (
        df_consolidado
        .join(F.broadcast(df_med_emi), on="SEC_EJEC", how="left")
        .withColumn(
            "Ind_Outlier_Saldo",
            F.when(
                (F.col("VIG_SALDO") == 1) & (F.col("MON_SALDOPREDIAL_ORD") > F.lit(1000000000)),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "Ind_Outlier_Emision",
            F.when(
                (F.col("MES_ESTADISTICA") == 13)
                & (F.col("VIG_EMISION") == 1)
                & (F.col("mediana_emision_muni") > F.lit(0.0))
                & (F.col("MON_EMISIONPREDIAL_AFECTO").cast("double") >= F.lit(50.0) * F.col("mediana_emision_muni"))
                & (F.col("MON_EMISIONPREDIAL_AFECTO") > F.lit(1000000)),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
        .drop("mediana_emision_muni")
    )

    ruta_dst = f"{SILVER_BASE}/sismepre/rentas_esat_estadistica_atm"
    df_final.write.mode("overwrite").parquet(ruta_dst)

    filas_escritas = spark.read.parquet(ruta_dst).count()
    t_fin = datetime.now()

    registrar_auditoria(
        spark=spark,
        id_ejecucion=id_ejecucion,
        capa="SILVER",
        proceso_script="02_silver_limpieza.py",
        fuente_tabla="silver/sismepre/rentas_esat_estadistica_atm",
        fecha_inicio=t_ini,
        fecha_fin=t_fin,
        filas_leidas=filas_leidas,
        filas_escritas=filas_escritas,
        filas_descartadas=filas_arbitrios_desc,
        estado="EXITOSO",
        observaciones="Excluidos 38,749 Arbitrios por TITULO; VIG_EMISION/VIG_RECAUD/VIG_SALDO; cierres reconstruidos M1-M12; Ind_Outlier_Saldo e Ind_Outlier_Emision",
    )


def limpiar_renamu(spark: SparkSession, id_ejecucion: str) -> None:
    """Normaliza Ubigeo a 6 dígitos y extrae variables censales clave de RENAMU 2022."""
    t_ini = datetime.now()
    df_bronze = spark.read.parquet(f"{BRONZE_BASE}/renamu/renamu_2022")
    filas_leidas = df_bronze.count()

    # Seleccionar columnas clave censales para evitar manipular las 1,368 columnas innecesariamente
    cols_clave = [
        "Año", "idmunici", "ccdd", "ccpp", "ccdi", "Ubigeo",
        "Departamento", "Provincia", "Distrito", "Tipomuni",
        "P32_1_T", "P17_7", "P17_8", "P31_3", "P33A", "P19D_T", "P16_5",
        "_archivo_origen",
    ]
    df_sel = limpiar_cols_texto(df_bronze.select(*cols_clave), {"_archivo_origen"})

    df_silver = (
        df_sel
        .withColumn("Anio_Censo", F.col("Año").cast(IntegerType()))
        .withColumn("idmunici", F.lpad(F.col("idmunici"), 6, "0"))
        .withColumn("ccdd", F.lpad(F.col("ccdd"), 2, "0"))
        .withColumn("ccpp", F.lpad(F.col("ccpp"), 2, "0"))
        .withColumn("ccdi", F.lpad(F.col("ccdi"), 2, "0"))
        .withColumn("Ubigeo", F.lpad(F.col("Ubigeo"), 6, "0"))
        .withColumn("Departamento", F.upper(F.col("Departamento")))
        .withColumn("Provincia", F.upper(F.col("Provincia")))
        .withColumn("Distrito", F.upper(F.col("Distrito")))
        .withColumn("Tipomuni", F.col("Tipomuni").cast(IntegerType()))
        .withColumn(
            "P32_1_T",
            F.when(F.col("P32_1_T").isNotNull(), F.col("P32_1_T").cast(IntegerType())),
        )
        .withColumn("P32_1_T_Personal_Trib", F.coalesce(F.col("P32_1_T"), F.lit(0)))
        .withColumn(
            "P19D_T",
            F.when(F.col("P19D_T").isNotNull(), F.col("P19D_T").cast(IntegerType())).otherwise(F.lit(0)),
        )
        .withColumn("Personal_Total", F.when(F.col("P19D_T") < 0, F.lit(0)).otherwise(F.col("P19D_T")))
        .withColumn("P17_7", F.when(F.col("P17_7").isin("1", "7"), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("Software_Tributario", F.col("P17_7"))
        .withColumn("P17_8", F.when(F.col("P17_8").isin("1", "8"), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("Catastro_Urbano", F.col("P17_8"))
        .withColumn("P31_3", F.when(F.col("P31_3").isin("1", "3"), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("Pasarela_Pagos_Web", F.col("P31_3"))
        .withColumn("P16_5", F.when(F.col("P16_5").isin("1", "5"), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("P33A_Cod", F.col("P33A").cast(IntegerType()))
        .withColumn("P33A", F.when(F.col("P33A").isin("1", "2"), F.lit(1)).otherwise(F.lit(0)))
        .withColumn(
            "Indice_TIC_Rentas",
            F.round((F.col("P17_7") + F.col("P17_8") + F.col("P31_3")) / F.lit(3.0) * F.lit(100.0), 2),
        )
        .drop("Año")
    )

    ruta_dst = f"{SILVER_BASE}/renamu/renamu_2022"
    df_silver.write.mode("overwrite").parquet(ruta_dst)

    filas_escritas = spark.read.parquet(ruta_dst).count()
    t_fin = datetime.now()

    assert filas_escritas == 1874, f"RENAMU Silver esperaba 1,874 filas, obtuvo {filas_escritas:,}"
    registrar_auditoria(
        spark=spark,
        id_ejecucion=id_ejecucion,
        capa="SILVER",
        proceso_script="02_silver_limpieza.py",
        fuente_tabla="silver/renamu/renamu_2022",
        fecha_inicio=t_ini,
        fecha_fin=t_fin,
        filas_leidas=filas_leidas,
        filas_escritas=filas_escritas,
        filas_descartadas=0,
        estado="EXITOSO",
        observaciones="Ubigeo LPAD 6 dígitos + variables clave censales Tipomuni, P32_1_T, P17_7, P17_8, P31_3, P33A, P19D_T",
    )


def main() -> None:
    id_ejecucion = f"SILVER_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:6]}"
    spark = crear_spark()
    try:
        limpiar_siaf(spark, id_ejecucion)
        limpiar_sismepre_tablas_maestras(spark, id_ejecucion)
        limpiar_esat(spark, id_ejecucion)
        limpiar_renamu(spark, id_ejecucion)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
