"""Capa Bronze: Ingesta cruda a Parquet sin inferSchema para SIAF, SISMEPRE y RENAMU."""

import csv
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path
from uuid import uuid4

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from audit_logger import AUDIT_PATH, registrar_auditoria

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_BASE = str(PROJECT_ROOT / "raw")
BRONZE_BASE = str(PROJECT_ROOT / "bronze")


def str_schema(columns: list[str]) -> StructType:
    """Construye un StructType explícito donde todas las columnas son StringType."""
    return StructType([StructField(col_name, StringType(), True) for col_name in columns])


SIAF_COLS = [
    "ANO_DOC", "MES_DOC", "NIVEL_GOBIERNO", "NIVEL_GOBIERNO_NOMBRE",
    "SECTOR", "SECTOR_NOMBRE", "PLIEGO", "PLIEGO_NOMBRE",
    "SEC_EJEC", "EJECUTORA", "EJECUTORA_NOMBRE",
    "DEPARTAMENTO_EJECUTORA", "DEPARTAMENTO_EJECUTORA_NOMBRE",
    "PROVINCIA_EJECUTORA", "PROVINCIA_EJECUTORA_NOMBRE",
    "DISTRITO_EJECUTORA", "DISTRITO_EJECUTORA_NOMBRE",
    "FUENTE_FINANCIAMIENTO", "FUENTE_FINANCIAMIENTO_NOMBRE",
    "RUBRO", "RUBRO_NOMBRE", "TIPO_RECURSO", "TIPO_RECURSO_NOMBRE",
    "GENERICA", "GENERICA_NOMBRE", "SUBGENERICA", "SUBGENERICA_NOMBRE",
    "SUBGENERICA_DET", "SUBGENERICA_DET_NOMBRE",
    "ESPECIFICA", "ESPECIFICA_NOMBRE", "ESPECIFICA_DET", "ESPECIFICA_DET_NOMBRE",
    "MONTO_PIA", "MONTO_PIM", "MONTO_RECAUDADO",
]

SISMEPRE_TABLAS = {
    "rentas_ano_aplicacion": (
        [
            "ANO_APLICACION", "ANO_APLICACION_INICIO", "ANO_APLICACION_FIN",
            "FECHA_CIERRE", "ESTADO", "PERIODO", "FECHA_PRES_OFICIO",
            "FECHA_INI_CIERRE", "FECHA_ING",
        ],
        27,
    ),
    "rentas_entidad_estado": (
        [
            "SEC_EJEC", "ANO_APLICACION", "USUARIO_CREACION_FECHA", "ESTADO",
            "USUARIO_ENVIO_ID", "USUARIO_FECHA_ENVIO", "CORREO",
            "ORIGEN_INFORMACION", "CLASIFICACION", "PERIODO", "TIPO_META",
            "IND_RESOL_ALCAL_ADJUNTO", "FECHA_RESOL_ALCAL_ADJUNTO",
        ],
        20391,
    ),
    "rentas_esat_estadistica_atm": (
        [
            "SEC_EJEC", "UBIGEO", "DEPARTAMENTO", "DEPARTAMENTO_NOMBRE",
            "PROVINCIA", "PROVINCIA_NOMBRE", "DISTRITO", "DISTRITO_NOMBRE",
            "MUNICIPALIDAD_NOMBRE", "ANO_APLICACION", "PERIODO", "ANO_ESTADISTICA",
            "MON_EMISIONPREDIAL_AFECTO", "MON_EMISIONPREDIAL_EXON",
            "NUM_EMISIONPREDIAL_AFECTO", "NUM_EMISIONPREDIAL_EXON",
            "NUM_EMISIONPREDIAL_CASA", "NUM_EMISIONPREDIAL_OTROS",
            "MON_BASEIMPONIBLE_AFECTO", "MON_BASEIMPONIBLE_EXON",
            "MON_AUTOAVALUO_INAFECTO", "MON_RECAUDACTUAL_ORDIN",
            "MON_RECAUDACTUAL_COAC", "MON_RECAUDANTER_ORDI",
            "MON_RECAUDANTER_COAC", "MON_INICIALADULTOMAYOR",
            "MON_PREDIALADULTOMAYOR", "NUM_CONTRIBADULTOMAYOR",
            "MON_RECUADADULTOMAYOR", "MON_SALDOPREDIAL_ORD",
            "MON_SALDOPREDIAL_COAC", "NUM_INAFECTOS", "TIPO_META",
            "NUM_CONTRIPREDIO", "FLAG_EMILIQUIDA", "NUM_PREDIOUSOCH",
            "NUM_PREDIOOTROUSO", "NUM_PREDIOTOTAL", "FLAG_EMISION_INICIAL",
            "FORMULARIO_ID", "MES_ESTADISTICA", "MON_EMISIONPREDIAL_INSO",
        ],
        149298,
    ),
    "rentas_estadistica": (
        [
            "ANO_APLICACION", "PERIODO", "FORMULARIO_ID",
            "ANO_ESTADISTICA", "MES_ESTADISTICA", "ESTADO_REGISTRO",
            "ANO_ESTADISTICA_DESC",
        ],
        233,
    ),
    "rentas_formulario": (
        [
            "ANO_APLICACION", "PERIODO", "FORMULARIO_ID", "ORDEN_FORMULARIO",
            "TITULO", "SUB_TITULO", "ABREVIATURA", "CLASIFICACION",
            "TIPO_FORMULARIO", "ESTADO_REGISTRO",
        ],
        98,
    ),
    "rentas_preguntas": (
        [
            "ANO_APLICACION", "PERIODO", "FORMULARIO_ID", "PREGUNTA_ID",
            "PREGUNTA_PADRE_ID", "ORDEN_PREGUNTA", "DESCRIPCION",
            "OBJETO_ACTIVO", "TIPO_CUESTIONARIO_ID", "RESPUESTA",
            "RANGO_INI", "RANGO_FIN", "TEXTO_APOYO", "TEXTO_LECTURA",
            "ESTADO_REGISTRO",
        ],
        836,
    ),
}

RESPUESTAS_COLS = [
    "SEC_EJEC", "ANO_APLICACION", "PERIODO", "FORMULARIO_ID",
    "PREGUNTA_ID", "RESPUESTA_ID", "RESPUESTA_TEXTO",
    "RESPUESTA_DECIMAL", "RESPUESTA_ENTERO", "RESPUESTA_FECHA",
    "ESTADO_REGISTRO",
]


def crear_spark() -> SparkSession:
    """Inicializa SparkSession replicando la configuración estándar del curso."""
    spark = (
        SparkSession.builder
        .appName("ProyectoFinal_01_Bronze_Ingesta")
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


def ingestar_siaf(spark: SparkSession, id_ejecucion: str) -> None:
    """Ingesta los 20 CSV del SIAF a bronze/siaf preservando todo como texto."""
    t_ini = datetime.now()
    ruta_src = f"{RAW_BASE}/siaf/*.csv"
    ruta_dst = f"{BRONZE_BASE}/siaf"

    df_raw = (
        spark.read
        .option("header", "true")
        .option("encoding", "UTF-8")
        .option("quote", '"')
        .option("escape", '"')
        .schema(str_schema(SIAF_COLS))
        .csv(ruta_src)
        .withColumn("_archivo_origen", F.input_file_name())
    )
    df_raw.write.mode("overwrite").parquet(ruta_dst)

    filas_escritas = spark.read.parquet(ruta_dst).count()
    t_fin = datetime.now()

    assert filas_escritas == 16719553, (
        f"SIAF Bronze esperaba 16,719,553 filas, obtuvo {filas_escritas:,}"
    )
    registrar_auditoria(
        spark=spark,
        id_ejecucion=id_ejecucion,
        capa="BRONZE",
        proceso_script="01_bronze_ingesta.py",
        fuente_tabla="bronze/siaf",
        fecha_inicio=t_ini,
        fecha_fin=t_fin,
        filas_leidas=filas_escritas,
        filas_escritas=filas_escritas,
        filas_descartadas=0,
        estado="EXITOSO",
        observaciones="20 CSV SIAF (2009-2026) con StructType StringType y _archivo_origen",
    )


def ingestar_sismepre_estandar(spark: SparkSession, id_ejecucion: str) -> None:
    """Ingesta las 6 tablas CSV estándar de SISMEPRE a bronze/sismepre/."""
    for tabla, (cols, esperado) in SISMEPRE_TABLAS.items():
        t_ini = datetime.now()
        ruta_src = f"{RAW_BASE}/sismepre/{tabla}.csv"
        ruta_dst = f"{BRONZE_BASE}/sismepre/{tabla}"

        df_raw = (
            spark.read
            .option("header", "true")
            .option("encoding", "UTF-8")
            .option("quote", '"')
            .option("escape", '"')
            .option("multiLine", "true")
            .schema(str_schema(cols))
            .csv(ruta_src)
            .withColumn("_archivo_origen", F.input_file_name())
        )
        df_raw.write.mode("overwrite").parquet(ruta_dst)

        filas_escritas = spark.read.parquet(ruta_dst).count()
        t_fin = datetime.now()

        assert filas_escritas == esperado, (
            f"{tabla} esperaba {esperado:,} filas, obtuvo {filas_escritas:,}"
        )
        registrar_auditoria(
            spark=spark,
            id_ejecucion=id_ejecucion,
            capa="BRONZE",
            proceso_script="01_bronze_ingesta.py",
            fuente_tabla=f"bronze/sismepre/{tabla}",
            fecha_inicio=t_ini,
            fecha_fin=t_fin,
            filas_leidas=filas_escritas,
            filas_escritas=filas_escritas,
            filas_descartadas=0,
            estado="EXITOSO",
            observaciones=f"Tabla SISMEPRE ({len(cols)} cols) leída con StructType StringType",
        )


def ingestar_rentas_respuestas(spark: SparkSession, id_ejecucion: str) -> None:
    """Ingesta rentas_respuestas.csv remediando comillas dobles internas sin escapar."""
    t_ini = datetime.now()
    ruta_src = f"{RAW_BASE}/sismepre/rentas_respuestas.csv"
    ruta_dst = f"{BRONZE_BASE}/sismepre/rentas_respuestas"

    df_txt = (
        spark.read.text(ruta_src)
        .withColumn("_archivo_origen", F.input_file_name())
        .filter(~F.col("value").startswith('"SEC_EJEC"'))
        .filter(F.length(F.trim(F.col("value"))) > 0)
    )
    filas_leidas = df_txt.count()

    # Tokenización posicional: las 6 primeras y las 4 últimas columnas tienen longitud fija
    stripped = F.regexp_replace(F.col("value"), r'^"|"\r?$', "")
    parts = F.split(stripped, '","')
    n_parts = F.size(parts)

    texto_reconstruido = F.when(
        n_parts > 11,
        F.concat_ws('","', F.slice(parts, 7, n_parts - 10)),
    ).otherwise(parts.getItem(6))
    texto_limpio = F.regexp_replace(
        F.regexp_replace(texto_reconstruido, r'"', "'"),
        r"[\r\n]+",
        " ",
    )

    df_parsed = df_txt.select(
        parts.getItem(0).cast(StringType()).alias("SEC_EJEC"),
        parts.getItem(1).cast(StringType()).alias("ANO_APLICACION"),
        parts.getItem(2).cast(StringType()).alias("PERIODO"),
        parts.getItem(3).cast(StringType()).alias("FORMULARIO_ID"),
        parts.getItem(4).cast(StringType()).alias("PREGUNTA_ID"),
        parts.getItem(5).cast(StringType()).alias("RESPUESTA_ID"),
        texto_limpio.cast(StringType()).alias("RESPUESTA_TEXTO"),
        parts.getItem(n_parts - 4).cast(StringType()).alias("RESPUESTA_DECIMAL"),
        parts.getItem(n_parts - 3).cast(StringType()).alias("RESPUESTA_ENTERO"),
        parts.getItem(n_parts - 2).cast(StringType()).alias("RESPUESTA_FECHA"),
        parts.getItem(n_parts - 1).cast(StringType()).alias("ESTADO_REGISTRO"),
        F.col("_archivo_origen"),
    )
    df_parsed.write.mode("overwrite").parquet(ruta_dst)

    filas_escritas = spark.read.parquet(ruta_dst).count()
    t_fin = datetime.now()

    assert filas_escritas == 322675, (
        f"rentas_respuestas esperaba 322,675 filas, obtuvo {filas_escritas:,}"
    )
    registrar_auditoria(
        spark=spark,
        id_ejecucion=id_ejecucion,
        capa="BRONZE",
        proceso_script="01_bronze_ingesta.py",
        fuente_tabla="bronze/sismepre/rentas_respuestas",
        fecha_inicio=t_ini,
        fecha_fin=t_fin,
        filas_leidas=filas_leidas,
        filas_escritas=filas_escritas,
        filas_descartadas=filas_leidas - filas_escritas,
        estado="EXITOSO",
        observaciones="Tokenización posicional parts[:6]/parts[6:-4]/parts[-4:] para 9 filas con comillas sin escapar",
    )


def ingestar_renamu(spark: SparkSession, id_ejecucion: str) -> None:
    """Ingesta Base_RENAMU_2022_f.csv (1,368 cols, sep ';', BOM utf-8-sig limpiado)."""
    t_ini = datetime.now()
    ruta_src = f"{RAW_BASE}/renamu/Base_RENAMU_2022_f.csv"
    ruta_dst = f"{BRONZE_BASE}/renamu/renamu_2022"

    # Leer encabezados con utf-8-sig para eliminar BOM (\ufeff) y construir StructType explícito
    with open(ruta_src, "r", encoding="utf-8-sig") as f:
        headers = next(csv.reader(f, delimiter=";"))

    headers = [h.lstrip("\ufeff") for h in headers]
    renamu_schema = str_schema(headers)

    # Desactivar wholeStage temporalmente para evitar overhead Janino sobre 1,368 columnas
    spark.conf.set("spark.sql.codegen.wholeStage", "false")
    try:
        # Nota: Spark JVM usa Charset 'UTF-8' (univocity remueve BOM), mientras Python usa utf-8-sig en headers
        df_raw = (
            spark.read
            .option("header", "true")
            .option("sep", ";")
            .option("encoding", "UTF-8")
            .option("quote", '"')
            .option("escape", '"')
            .schema(renamu_schema)
            .csv(ruta_src)
            .withColumn(headers[0], F.regexp_replace(F.col(headers[0]), "^\ufeff", ""))
            .withColumn("_archivo_origen", F.input_file_name())
        )
        df_raw.write.mode("overwrite").parquet(ruta_dst)
    finally:
        spark.conf.set("spark.sql.codegen.wholeStage", "true")

    filas_escritas = spark.read.parquet(ruta_dst).count()
    t_fin = datetime.now()

    assert filas_escritas == 1874, (
        f"RENAMU 2022 esperaba 1,874 filas, obtuvo {filas_escritas:,}"
    )
    registrar_auditoria(
        spark=spark,
        id_ejecucion=id_ejecucion,
        capa="BRONZE",
        proceso_script="01_bronze_ingesta.py",
        fuente_tabla="bronze/renamu/renamu_2022",
        fecha_inicio=t_ini,
        fecha_fin=t_fin,
        filas_leidas=filas_escritas,
        filas_escritas=filas_escritas,
        filas_descartadas=0,
        estado="EXITOSO",
        observaciones=f"Censo completo RENAMU 2022 ({len(headers)} cols, sep=';', BOM utf-8-sig limpiado)",
    )


def main() -> None:
    if os.path.exists(AUDIT_PATH):
        shutil.rmtree(AUDIT_PATH, ignore_errors=True)

    id_ejecucion = f"BRONZE_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:6]}"
    spark = crear_spark()
    try:
        ingestar_siaf(spark, id_ejecucion)
        ingestar_sismepre_estandar(spark, id_ejecucion)
        ingestar_rentas_respuestas(spark, id_ejecucion)
        ingestar_renamu(spark, id_ejecucion)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
