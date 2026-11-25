"""Capa Gold: Modelado dimensional Galaxy Schema (Constelación de Hechos) para Power BI."""

import os
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from audit_logger import registrar_auditoria

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SILVER_BASE = str(PROJECT_ROOT / "silver")
GOLD_BASE = str(PROJECT_ROOT / "gold")

REGIONES_NATURALES = {
    "01": "Selva",
    "02": "Sierra",
    "03": "Sierra",
    "04": "Costa",
    "05": "Sierra",
    "06": "Sierra",
    "07": "Costa",
    "08": "Sierra",
    "09": "Sierra",
    "10": "Sierra",
    "11": "Costa",
    "12": "Sierra",
    "13": "Costa",
    "14": "Costa",
    "15": "Costa",
    "16": "Selva",
    "17": "Selva",
    "18": "Costa",
    "19": "Sierra",
    "20": "Costa",
    "21": "Sierra",
    "22": "Selva",
    "23": "Costa",
    "24": "Costa",
    "25": "Selva",
}

MESES_ES = {
    1: "Enero",
    2: "Febrero",
    3: "Marzo",
    4: "Abril",
    5: "Mayo",
    6: "Junio",
    7: "Julio",
    8: "Agosto",
    9: "Septiembre",
    10: "Octubre",
    11: "Noviembre",
    12: "Diciembre",
}


def crear_spark() -> SparkSession:
    """Inicializa SparkSession replicando la configuración estándar del curso."""
    spark = (
        SparkSession.builder
        .appName("ProyectoFinal_03_Gold_Modelo")
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


def construir_dim_tiempo(spark: SparkSession, id_ejecucion: str) -> DataFrame:
    """Genera calendario diario continuo 2009-2026 + 18 filas de cierre anual Mes_Numero=13."""
    t_ini = datetime.now()
    map_meses = F.create_map([F.lit(x) for pair in MESES_ES.items() for x in pair])

    df_dias = (
        spark.sql("SELECT explode(sequence(to_date('2009-01-01'), to_date('2026-12-31'), interval 1 day)) AS Fecha")
        .withColumn("Fecha_Completa", F.col("Fecha"))
        .withColumn("Anio", F.year("Fecha").cast(IntegerType()))
        .withColumn("Mes_Numero", F.month("Fecha").cast(IntegerType()))
        .withColumn("Dia", F.dayofmonth("Fecha").cast(IntegerType()))
        .withColumn(
            "SK_Fecha",
            (F.col("Anio") * 10000 + F.col("Mes_Numero") * 100 + F.col("Dia")).cast(IntegerType()),
        )
        .withColumn("MesNombre", map_meses[F.col("Mes_Numero")])
        .withColumn("Trimestre", F.concat(F.lit("T"), F.quarter("Fecha")))
        .withColumn("Es_Cierre_Anual", F.lit(False))
        .withColumn("ConDatos", F.col("Fecha") <= F.to_date(F.lit("2026-11-30")))
        .drop("Dia")
    )

    df_cierres = (
        spark.sql("SELECT explode(sequence(2009, 2026)) AS Anio")
        .withColumn("Anio", F.col("Anio").cast(IntegerType()))
        .withColumn("Fecha", F.make_date(F.col("Anio"), F.lit(12), F.lit(31)))
        .withColumn("Fecha_Completa", F.col("Fecha"))
        .withColumn("Mes_Numero", F.lit(13).cast(IntegerType()))
        .withColumn("SK_Fecha", (F.col("Anio") * 10000 + 1301).cast(IntegerType()))
        .withColumn("MesNombre", F.lit("Cierre Anual"))
        .withColumn("Trimestre", F.lit("Cierre"))
        .withColumn("Es_Cierre_Anual", F.lit(True))
        .withColumn("ConDatos", F.col("Anio") <= F.lit(2025))
    )

    df_dim = (
        df_dias
        .unionByName(df_cierres)
        .select(
            "SK_Fecha", "Fecha", "Fecha_Completa", "Anio",
            "Mes_Numero", "MesNombre", "Trimestre", "Es_Cierre_Anual", "ConDatos",
        )
    )

    ruta_dst = f"{GOLD_BASE}/dim_tiempo"
    df_dim.coalesce(1).write.mode("overwrite").parquet(ruta_dst)

    df_out = spark.read.parquet(ruta_dst)
    filas = df_out.count()
    assert filas == 6592, f"dim_tiempo esperaba 6,592 filas (6,574 días + 18 cierres), obtuvo {filas}"

    registrar_auditoria(
        spark, id_ejecucion, "GOLD", "03_gold_modelo.py", "gold/dim_tiempo",
        t_ini, datetime.now(), filas, filas, 0,
        observaciones="Calendario continuo 2009-2026 (6,574 días) + 18 cierres anuales Mes_Numero=13",
    )
    return df_out


def construir_dim_geografia(
    spark: SparkSession,
    df_siaf: DataFrame,
    df_renamu: DataFrame,
    df_esat: DataFrame,
    id_ejecucion: str,
) -> DataFrame:
    """Unifica UBIGEO 6 dígitos de RENAMU 2022, SIAF post-2022, ESAT y miembro inferido 000000."""
    t_ini = datetime.now()
    map_reg = F.create_map([F.lit(x) for pair in REGIONES_NATURALES.items() for x in pair])

    df_geo_ren = (
        df_renamu
        .select(
            F.col("Ubigeo").alias("SK_Ubigeo"),
            F.col("ccdd").alias("Cod_Departamento"),
            F.col("Departamento"),
            F.col("Provincia"),
            F.col("Distrito"),
            F.lit("Censado RENAMU 2022").alias("Estado_Censal"),
        )
    )

    w_siaf_geo = Window.partitionBy("UBIGEO").orderBy(F.col("FECHA").desc())
    df_geo_siaf = (
        df_siaf
        .filter(F.col("UBIGEO").isNotNull())
        .withColumn("rn", F.row_number().over(w_siaf_geo))
        .filter(F.col("rn") == 1)
        .select(
            F.col("UBIGEO").alias("SK_Ubigeo"),
            F.col("DEPARTAMENTO_EJECUTORA").alias("Cod_Departamento"),
            F.col("DEPARTAMENTO_EJECUTORA_NOMBRE").alias("Departamento"),
            F.col("PROVINCIA_EJECUTORA_NOMBRE").alias("Provincia"),
            F.col("DISTRITO_EJECUTORA_NOMBRE").alias("Distrito"),
            F.lit("Distrito Nuevo Post-Censo 2022").alias("Estado_Censal"),
        )
    )

    w_esat_geo = Window.partitionBy("UBIGEO").orderBy(F.col("ANO_APLICACION").desc(), F.col("PERIODO").desc())
    df_geo_esat = (
        df_esat
        .filter(F.col("UBIGEO").isNotNull())
        .withColumn("rn", F.row_number().over(w_esat_geo))
        .filter(F.col("rn") == 1)
        .select(
            F.col("UBIGEO").alias("SK_Ubigeo"),
            F.col("DEPARTAMENTO").alias("Cod_Departamento"),
            F.col("DEPARTAMENTO_NOMBRE").alias("Departamento"),
            F.col("PROVINCIA_NOMBRE").alias("Provincia"),
            F.col("DISTRITO_NOMBRE").alias("Distrito"),
            F.lit("Catalogado SISMEPRE").alias("Estado_Censal"),
        )
    )

    df_solo_siaf = df_geo_siaf.join(df_geo_ren.select("SK_Ubigeo"), on="SK_Ubigeo", how="left_anti")
    df_base = df_geo_ren.unionByName(df_solo_siaf)
    df_solo_esat = df_geo_esat.join(df_base.select("SK_Ubigeo"), on="SK_Ubigeo", how="left_anti")
    df_unido = df_base.unionByName(df_solo_esat)

    df_desconocido = spark.createDataFrame(
        [("000000", "00", "SIN UBIGEO REGISTRADO", "SIN UBIGEO REGISTRADO", "SIN UBIGEO REGISTRADO", "Sin Ubigeo Registrado")],
        schema=df_unido.schema,
    )

    df_dim = (
        df_unido
        .unionByName(df_desconocido)
        .withColumn("UBIGEO", F.col("SK_Ubigeo"))
        .withColumn(
            "Region_Natural",
            F.coalesce(map_reg[F.col("Cod_Departamento")], F.lit("Sin Dato")),
        )
        .select(
            "SK_Ubigeo", "UBIGEO", "Cod_Departamento",
            "Departamento", "Provincia", "Distrito", "Region_Natural", "Estado_Censal",
        )
    )

    ruta_dst = f"{GOLD_BASE}/dim_geografia_ubigeo"
    df_dim.coalesce(1).write.mode("overwrite").parquet(ruta_dst)

    df_out = spark.read.parquet(ruta_dst)
    filas = df_out.count()
    registrar_auditoria(
        spark, id_ejecucion, "GOLD", "03_gold_modelo.py", "gold/dim_geografia_ubigeo",
        t_ini, datetime.now(), filas, filas, 0,
        observaciones="1,874 UBIGEO RENAMU + distritos nuevos post-2022 + miembro inferido 000000",
    )
    return df_out


def construir_dim_entidad(
    spark: SparkSession,
    df_siaf: DataFrame,
    df_renamu: DataFrame,
    df_estado_atm: DataFrame,
    id_ejecucion: str,
) -> DataFrame:
    """Construye dim_entidad_municipalidad (3,049 unidades ejecutoras, SCD1 + RENAMU + imputación mediana)."""
    t_ini = datetime.now()

    w_ent = Window.partitionBy("SEC_EJEC").orderBy(F.col("FECHA").desc(), F.col("EJECUTORA_NOMBRE").desc())
    df_ent_siaf = (
        df_siaf
        .withColumn("rn", F.row_number().over(w_ent))
        .filter(F.col("rn") == 1)
        .select(
            F.col("SEC_EJEC").cast(IntegerType()).alias("SK_Municipalidad"),
            F.col("SEC_EJEC").cast(IntegerType()).alias("SEC_EJEC"),
            F.col("SEC_EJEC").cast("string").alias("Cod_SecEjec"),
            F.col("EJECUTORA"),
            F.col("EJECUTORA_NOMBRE").alias("Nombre_Municipalidad"),
            F.col("EJECUTORA_NOMBRE"),
            F.col("NIVEL_GOBIERNO"),
            F.col("NIVEL_GOBIERNO_NOMBRE"),
            F.col("SECTOR"),
            F.col("SECTOR_NOMBRE"),
            F.col("PLIEGO"),
            F.col("PLIEGO_NOMBRE"),
            F.col("DEPARTAMENTO_EJECUTORA"),
            F.col("DEPARTAMENTO_EJECUTORA_NOMBRE"),
            F.col("PROVINCIA_EJECUTORA"),
            F.col("PROVINCIA_EJECUTORA_NOMBRE"),
            F.col("DISTRITO_EJECUTORA"),
            F.col("DISTRITO_EJECUTORA_NOMBRE"),
            F.coalesce(F.col("UBIGEO"), F.lit("000000")).alias("UBIGEO"),
        )
    )

    # Última clasificación ATM por SEC_EJEC
    w_clasif = Window.partitionBy("SEC_EJEC").orderBy(F.col("ANO_APLICACION").desc(), F.col("PERIODO").desc())
    df_clasif = (
        df_estado_atm
        .withColumn("rn", F.row_number().over(w_clasif))
        .filter(F.col("rn") == 1)
        .select(
            F.col("SEC_EJEC").cast(IntegerType()).alias("SEC_EJEC"),
            F.col("CLASIFICACION").alias("Clasificacion_ATM"),
            F.col("TIPO_META").alias("Tipo_Meta_ATM"),
        )
    )

    # Medianas por grupo (ccdd, Tipomuni) en RENAMU para imputación de los 20 distritos post-2022
    df_med_renamu = (
        df_renamu
        .groupBy(F.col("ccdd").alias("DEPARTAMENTO_EJECUTORA"), "Tipomuni")
        .agg(
            F.sort_array(F.collect_list("P32_1_T_Personal_Trib")).alias("arr_p32"),
            F.sort_array(F.collect_list("Personal_Total")).alias("arr_ptot"),
        )
        .withColumn("n_p32", F.size("arr_p32"))
        .withColumn("n_ptot", F.size("arr_ptot"))
        .withColumn(
            "mediana_p32_grupo",
            F.when(F.col("n_p32") % 2 == 1, F.element_at("arr_p32", (F.col("n_p32") / 2 + 1).cast("int")))
            .otherwise(
                F.round(
                    (
                        F.element_at("arr_p32", (F.col("n_p32") / 2).cast("int"))
                        + F.element_at("arr_p32", (F.col("n_p32") / 2 + 1).cast("int"))
                    ) / 2.0
                ).cast(IntegerType())
            ),
        )
        .withColumn(
            "mediana_ptot_grupo",
            F.when(F.col("n_ptot") % 2 == 1, F.element_at("arr_ptot", (F.col("n_ptot") / 2 + 1).cast("int")))
            .otherwise(
                F.round(
                    (
                        F.element_at("arr_ptot", (F.col("n_ptot") / 2).cast("int"))
                        + F.element_at("arr_ptot", (F.col("n_ptot") / 2 + 1).cast("int"))
                    ) / 2.0
                ).cast(IntegerType())
            ),
        )
        .select("DEPARTAMENTO_EJECUTORA", "Tipomuni", "mediana_p32_grupo", "mediana_ptot_grupo")
    )

    df_ren_join = df_renamu.select(
        F.col("Ubigeo").alias("UBIGEO"),
        F.col("Tipomuni").alias("REN_Tipomuni"),
        F.col("P32_1_T").alias("REN_P32_1_T"),
        F.col("P32_1_T_Personal_Trib").alias("REN_P32_1_T_Personal_Trib"),
        F.col("P19D_T").alias("REN_P19D_T"),
        F.col("Personal_Total").alias("REN_Personal_Total"),
        F.col("P17_7").alias("REN_P17_7"),
        F.col("P17_8").alias("REN_P17_8"),
        F.col("P31_3").alias("REN_P31_3"),
        F.col("P16_5").alias("REN_P16_5"),
        F.col("P33A").alias("REN_P33A"),
        F.col("P33A_Cod").alias("REN_P33A_Cod"),
        F.col("Indice_TIC_Rentas").alias("REN_Indice_TIC_Rentas"),
    )

    df_joined = (
        df_ent_siaf
        .join(F.broadcast(df_clasif), on="SEC_EJEC", how="left")
        .join(F.broadcast(df_ren_join), on="UBIGEO", how="left")
    )

    es_muni = F.col("NIVEL_GOBIERNO") == F.lit("M")
    tipomuni_calc = F.when(
        es_muni,
        F.coalesce(F.col("REN_Tipomuni"), F.lit(2)),
    )

    df_with_tipo = df_joined.withColumn("Tipomuni", tipomuni_calc)

    df_dim = (
        df_with_tipo
        .join(F.broadcast(df_med_renamu), on=["DEPARTAMENTO_EJECUTORA", "Tipomuni"], how="left")
        .withColumn("SK_Ubigeo", F.col("UBIGEO"))
        .withColumn(
            "Tipo_Municipalidad",
            F.when(F.col("Tipomuni") == 1, F.lit("Provincial"))
            .when(F.col("Tipomuni") == 2, F.lit("Distrital"))
            .otherwise(F.col("NIVEL_GOBIERNO_NOMBRE")),
        )
        .withColumn(
            "Estado_Censal",
            F.when(~es_muni, F.lit("Entidad No Municipal"))
            .when(F.col("UBIGEO") == F.lit("000000"), F.lit("Sin Ubigeo Registrado"))
            .when(F.col("REN_Tipomuni").isNotNull(), F.lit("Censado RENAMU 2022"))
            .otherwise(F.lit("Distrito Nuevo Post-Censo 2022")),
        )
        .withColumn(
            "P32_1_T_Personal_Trib",
            F.when(
                es_muni & (F.col("UBIGEO") != F.lit("000000")),
                F.coalesce(F.col("REN_P32_1_T_Personal_Trib"), F.col("mediana_p32_grupo"), F.lit(0)),
            ),
        )
        .withColumn(
            "P32_1_T",
            F.when(
                es_muni & (F.col("UBIGEO") != F.lit("000000")),
                F.coalesce(F.col("REN_P32_1_T"), F.col("mediana_p32_grupo")),
            ),
        )
        .withColumn(
            "Personal_Total",
            F.when(
                es_muni & (F.col("UBIGEO") != F.lit("000000")),
                F.coalesce(F.col("REN_Personal_Total"), F.col("mediana_ptot_grupo"), F.lit(0)),
            ),
        )
        .withColumn(
            "P19D_T",
            F.when(
                es_muni & (F.col("UBIGEO") != F.lit("000000")),
                F.coalesce(F.col("REN_P19D_T"), F.col("mediana_ptot_grupo"), F.lit(0)),
            ),
        )
        .withColumn("P17_7", F.when(es_muni, F.coalesce(F.col("REN_P17_7"), F.lit(0))))
        .withColumn("Software_Tributario", F.col("P17_7"))
        .withColumn("P17_8", F.when(es_muni, F.coalesce(F.col("REN_P17_8"), F.lit(0))))
        .withColumn("Catastro_Urbano", F.col("P17_8"))
        .withColumn("P31_3", F.when(es_muni, F.coalesce(F.col("REN_P31_3"), F.lit(0))))
        .withColumn("Pasarela_Pagos_Web", F.col("P31_3"))
        .withColumn("P16_5", F.when(es_muni, F.coalesce(F.col("REN_P16_5"), F.lit(0))))
        .withColumn("P33A", F.when(es_muni, F.coalesce(F.col("REN_P33A"), F.lit(0))))
        .withColumn("P33A_Cod", F.when(es_muni, F.col("REN_P33A_Cod")))
        .withColumn(
            "Indice_TIC_Rentas",
            F.when(es_muni, F.coalesce(F.col("REN_Indice_TIC_Rentas"), F.lit(0.0))),
        )
        .select(
            "SK_Municipalidad", "SEC_EJEC", "Cod_SecEjec", "UBIGEO", "SK_Ubigeo",
            "Nombre_Municipalidad", "EJECUTORA", "EJECUTORA_NOMBRE",
            "NIVEL_GOBIERNO", "NIVEL_GOBIERNO_NOMBRE", "SECTOR", "SECTOR_NOMBRE",
            "PLIEGO", "PLIEGO_NOMBRE",
            "DEPARTAMENTO_EJECUTORA", "DEPARTAMENTO_EJECUTORA_NOMBRE",
            "PROVINCIA_EJECUTORA", "PROVINCIA_EJECUTORA_NOMBRE",
            "DISTRITO_EJECUTORA", "DISTRITO_EJECUTORA_NOMBRE",
            "Tipomuni", "Tipo_Municipalidad", "Clasificacion_ATM", "Tipo_Meta_ATM",
            "P32_1_T", "P32_1_T_Personal_Trib", "P19D_T", "Personal_Total",
            "P17_7", "Software_Tributario", "P17_8", "Catastro_Urbano",
            "P31_3", "Pasarela_Pagos_Web", "P16_5", "P33A", "P33A_Cod",
            "Indice_TIC_Rentas", "Estado_Censal",
        )
    )

    ruta_dst = f"{GOLD_BASE}/dim_entidad_municipalidad"
    df_dim.coalesce(1).write.mode("overwrite").parquet(ruta_dst)

    df_out = spark.read.parquet(ruta_dst)
    filas = df_out.count()
    no_muni = df_out.filter(F.col("NIVEL_GOBIERNO") != F.lit("M")).count()
    assert filas > 0, "dim_entidad_municipalidad resultó vacía"
    assert no_muni == 0, f"Se detectaron {no_muni} entidades con NIVEL_GOBIERNO != 'M'"

    registrar_auditoria(
        spark, id_ejecucion, "GOLD", "03_gold_modelo.py", "gold/dim_entidad_municipalidad",
        t_ini, datetime.now(), filas, filas, 0,
        observaciones=f"{filas:,} municipalidades (NIVEL_GOBIERNO='M') + Clasificacion_ATM + RENAMU 2022 + imputación mediana post-2022",
    )
    return df_out


def construir_dim_clasificador(spark: SparkSession, df_siaf: DataFrame, id_ejecucion: str) -> DataFrame:
    """Construye dim_clasificador_ingreso con clasificadores municipales (SCD1 último nombre)."""
    t_ini = datetime.now()
    w = Window.partitionBy("CLASIFICADOR").orderBy(F.col("FECHA").desc(), F.col("ESPECIFICA_DET_NOMBRE").desc())
    w_sk = Window.orderBy("CLASIFICADOR")

    df_dim = (
        df_siaf
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .withColumn("SK_Clasificador", F.row_number().over(w_sk).cast(IntegerType()))
        .select(
            "SK_Clasificador", "CLASIFICADOR",
            "GENERICA", "GENERICA_NOMBRE",
            "SUBGENERICA", "SUBGENERICA_NOMBRE",
            "SUBGENERICA_DET", "SUBGENERICA_DET_NOMBRE",
            "ESPECIFICA", "ESPECIFICA_NOMBRE",
            "ESPECIFICA_DET", "ESPECIFICA_DET_NOMBRE",
            "ES_PREDIAL",
        )
    )

    ruta_dst = f"{GOLD_BASE}/dim_clasificador_ingreso"
    df_dim.coalesce(1).write.mode("overwrite").parquet(ruta_dst)

    df_out = spark.read.parquet(ruta_dst)
    filas = df_out.count()
    assert filas > 0, "dim_clasificador_ingreso resultó vacía"

    registrar_auditoria(
        spark, id_ejecucion, "GOLD", "03_gold_modelo.py", "gold/dim_clasificador_ingreso",
        t_ini, datetime.now(), filas, filas, 0,
        observaciones=f"{filas:,} clasificadores presupuestales municipales (5 niveles, SCD1)",
    )
    return df_out


def construir_dim_fuente_rubro(spark: SparkSession, df_siaf: DataFrame, id_ejecucion: str) -> DataFrame:
    """Construye dim_fuente_rubro con rubros presupuestales municipales."""
    t_ini = datetime.now()
    w = Window.partitionBy("RUBRO").orderBy(F.col("FECHA").desc(), F.col("RUBRO_NOMBRE").desc())
    w_sk = Window.orderBy("RUBRO")

    df_dim = (
        df_siaf
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .withColumn("SK_Fuente_Rubro", F.row_number().over(w_sk).cast(IntegerType()))
        .select(
            "SK_Fuente_Rubro", "RUBRO", "RUBRO_NOMBRE",
            "FUENTE_FINANCIAMIENTO", "FUENTE_FINANCIAMIENTO_NOMBRE",
        )
    )

    ruta_dst = f"{GOLD_BASE}/dim_fuente_rubro"
    df_dim.coalesce(1).write.mode("overwrite").parquet(ruta_dst)

    df_out = spark.read.parquet(ruta_dst)
    filas = df_out.count()
    assert filas > 0, "dim_fuente_rubro resultó vacía"

    registrar_auditoria(
        spark, id_ejecucion, "GOLD", "03_gold_modelo.py", "gold/dim_fuente_rubro",
        t_ini, datetime.now(), filas, filas, 0,
        observaciones=f"{filas:,} rubros de financiamiento SIAF Municipal",
    )
    return df_out


def construir_dim_tipo_recurso(spark: SparkSession, df_siaf: DataFrame, id_ejecucion: str) -> DataFrame:
    """Construye dim_tipo_recurso con subcuentas RUBRO.TIPO_RECURSO municipales."""
    t_ini = datetime.now()
    w = Window.partitionBy("TIPO_RECURSO_KEY").orderBy(F.col("FECHA").desc(), F.col("TIPO_RECURSO_NOMBRE").desc())
    w_sk = Window.orderBy("TIPO_RECURSO_KEY")

    df_dim = (
        df_siaf
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .withColumn("SK_Tipo_Recurso", F.row_number().over(w_sk).cast(IntegerType()))
        .select(
            "SK_Tipo_Recurso", "TIPO_RECURSO_KEY", "RUBRO",
            "TIPO_RECURSO", "TIPO_RECURSO_NOMBRE",
        )
    )

    ruta_dst = f"{GOLD_BASE}/dim_tipo_recurso"
    df_dim.coalesce(1).write.mode("overwrite").parquet(ruta_dst)

    df_out = spark.read.parquet(ruta_dst)
    filas = df_out.count()
    assert filas > 0, "dim_tipo_recurso resultó vacía"

    registrar_auditoria(
        spark, id_ejecucion, "GOLD", "03_gold_modelo.py", "gold/dim_tipo_recurso",
        t_ini, datetime.now(), filas, filas, 0,
        observaciones=f"{filas:,} subcuentas municipales RUBRO.TIPO_RECURSO",
    )
    return df_out


def construir_dim_formulario_pregunta(spark: SparkSession, id_ejecucion: str) -> DataFrame:
    """Construye dim_formulario_pregunta uniendo rentas_preguntas (836) con rentas_formulario."""
    t_ini = datetime.now()
    df_preg = spark.read.parquet(f"{SILVER_BASE}/sismepre/rentas_preguntas")
    df_form = spark.read.parquet(f"{SILVER_BASE}/sismepre/rentas_formulario").select(
        "ANO_APLICACION", "PERIODO", "FORMULARIO_ID",
        "ORDEN_FORMULARIO", "TITULO", "SUB_TITULO",
        "ABREVIATURA", "CLASIFICACION", "TIPO_FORMULARIO",
    )

    w_sk = Window.orderBy("ANO_APLICACION", "PERIODO", "FORMULARIO_ID", "PREGUNTA_ID")
    df_dim = (
        df_preg
        .join(F.broadcast(df_form), on=["ANO_APLICACION", "PERIODO", "FORMULARIO_ID"], how="left")
        .withColumn("SK_Pregunta", F.row_number().over(w_sk).cast(IntegerType()))
        .select(
            "SK_Pregunta", "ANO_APLICACION", "PERIODO", "FORMULARIO_ID",
            "ABREVIATURA", "TITULO", "SUB_TITULO", "TIPO_FORMULARIO", "CLASIFICACION",
            "PREGUNTA_ID", "PREGUNTA_PADRE_ID", "ORDEN_PREGUNTA",
            "DESCRIPCION", "TIPO_CUESTIONARIO_ID", "OBJETO_ACTIVO",
            "RANGO_INI", "RANGO_FIN", "TEXTO_APOYO", "TEXTO_LECTURA",
        )
    )

    ruta_dst = f"{GOLD_BASE}/dim_formulario_pregunta"
    df_dim.coalesce(1).write.mode("overwrite").parquet(ruta_dst)

    df_out = spark.read.parquet(ruta_dst)
    filas = df_out.count()
    assert filas == 836, f"dim_formulario_pregunta esperaba 836 filas, obtuvo {filas:,}"

    registrar_auditoria(
        spark, id_ejecucion, "GOLD", "03_gold_modelo.py", "gold/dim_formulario_pregunta",
        t_ini, datetime.now(), filas, filas, 0,
        observaciones="Catálogo de 836 preguntas ATM enlazado con formularios 2A, 2B, 2C",
    )
    return df_out


def construir_fact_ejecucion_ingresos(
    spark: SparkSession,
    df_siaf: DataFrame,
    df_dim_clasif: DataFrame,
    df_dim_rubro: DataFrame,
    df_dim_trecurso: DataFrame,
    id_ejecucion: str,
) -> None:
    """Construye fact_ejecucion_ingresos agrupando duplicados de grano y filtrando filas en cero."""
    t_ini = datetime.now()
    filas_leidas = df_siaf.count()

    df_filtrado = df_siaf.filter(~F.col("ES_FILA_SIN_MONTO"))

    df_agg = (
        df_filtrado
        .withColumn("UBIGEO_NORM", F.coalesce(F.col("UBIGEO"), F.lit("000000")))
        .groupBy(
            "FECHA", "ANO_DOC", "MES_DOC", "SEC_EJEC",
            "UBIGEO_NORM", "RUBRO", "TIPO_RECURSO_KEY", "CLASIFICADOR",
            "ES_EXTORNO_NEGATIVO",
        )
        .agg(
            F.sum("MONTO_PIA").cast(DecimalType(18, 2)).alias("MONTO_PIA"),
            F.sum("MONTO_PIM").cast(DecimalType(18, 2)).alias("MONTO_PIM"),
            F.sum("MONTO_RECAUDADO").cast(DecimalType(18, 2)).alias("MONTO_RECAUDADO"),
        )
    )

    df_fact = (
        df_agg
        .withColumn(
            "SK_Fecha",
            (F.col("ANO_DOC").cast(IntegerType()) * 10000 + F.col("MES_DOC").cast(IntegerType()) * 100 + 1).cast(IntegerType()),
        )
        .withColumn("SK_Municipalidad", F.col("SEC_EJEC").cast(IntegerType()))
        .withColumn("SK_Ubigeo", F.col("UBIGEO_NORM"))
        .join(F.broadcast(df_dim_clasif.select("CLASIFICADOR", "SK_Clasificador")), on="CLASIFICADOR", how="left")
        .join(F.broadcast(df_dim_rubro.select("RUBRO", "SK_Fuente_Rubro")), on="RUBRO", how="left")
        .join(F.broadcast(df_dim_trecurso.select("TIPO_RECURSO_KEY", "SK_Tipo_Recurso")), on="TIPO_RECURSO_KEY", how="left")
        .select(
            "SK_Fecha", "SK_Municipalidad", "SK_Ubigeo",
            "SK_Clasificador", "SK_Fuente_Rubro", "SK_Tipo_Recurso",
            "FECHA", "ANO_DOC", "MES_DOC", "SEC_EJEC",
            "RUBRO", "TIPO_RECURSO_KEY", "CLASIFICADOR", "ES_EXTORNO_NEGATIVO",
            "MONTO_PIA", "MONTO_PIM", "MONTO_RECAUDADO",
        )
    )

    ruta_dst = f"{GOLD_BASE}/fact_ejecucion_ingresos"
    df_fact.write.mode("overwrite").parquet(ruta_dst)

    filas_escritas = spark.read.parquet(ruta_dst).count()
    filas_descartadas = filas_leidas - filas_escritas
    t_fin = datetime.now()

    assert filas_escritas == 10401713, (
        f"fact_ejecucion_ingresos esperaba 10,401,713 filas municipales, obtuvo {filas_escritas:,}"
    )
    registrar_auditoria(
        spark, id_ejecucion, "GOLD", "03_gold_modelo.py", "gold/fact_ejecucion_ingresos",
        t_ini, t_fin, filas_leidas, filas_escritas, filas_descartadas,
        observaciones="Solo Municipalidades ('M'); filtrado NOT ES_FILA_SIN_MONTO (-2,334,146) + SUM por grano (-14 duplicados)",
    )


def construir_fact_metas_atm(
    spark: SparkSession,
    df_esat: DataFrame,
    id_ejecucion: str,
) -> None:
    """Construye fact_metas_estadistica_atm consolidando bloques vigentes y cumplimiento de meta."""
    t_ini = datetime.now()
    filas_leidas = df_esat.count()

    df_vig = df_esat.filter(
        (F.col("VIG_EMISION") == 1) | (F.col("VIG_RECAUD") == 1) | (F.col("VIG_SALDO") == 1)
    )

    df_agg = (
        df_vig
        .groupBy("SEC_EJEC", "ANO_ESTADISTICA", "MES_ESTADISTICA")
        .agg(
            F.coalesce(F.first("UBIGEO", ignorenulls=True), F.lit("000000")).alias("SK_Ubigeo"),
            F.sum(F.when(F.col("VIG_EMISION") == 1, F.col("MON_EMISIONPREDIAL_AFECTO")).otherwise(F.lit(0)))
            .cast(DecimalType(18, 2)).alias("Monto_Emision_Predial"),
            F.sum(F.when(F.col("VIG_RECAUD") == 1, F.col("MON_RECAUDACTUAL_ORDIN")).otherwise(F.lit(0)))
            .cast(DecimalType(18, 2)).alias("Monto_Recaudado_Ord"),
            F.sum(F.when(F.col("VIG_RECAUD") == 1, F.col("MON_RECAUDACTUAL_COAC")).otherwise(F.lit(0)))
            .cast(DecimalType(18, 2)).alias("Monto_Recaudado_Coact"),
            F.sum(F.when(F.col("VIG_RECAUD") == 1, F.col("MON_RECAUDANTER_ORDI")).otherwise(F.lit(0)))
            .cast(DecimalType(18, 2)).alias("Monto_Recaud_Anter_Ord"),
            F.sum(F.when(F.col("VIG_RECAUD") == 1, F.col("MON_RECAUDANTER_COAC")).otherwise(F.lit(0)))
            .cast(DecimalType(18, 2)).alias("Monto_Recaud_Anter_Coac"),
            F.sum(F.when(F.col("VIG_SALDO") == 1, F.col("MON_SALDOPREDIAL_ORD")).otherwise(F.lit(0)))
            .cast(DecimalType(18, 2)).alias("Monto_Saldo_Ord"),
            F.sum(F.when(F.col("VIG_SALDO") == 1, F.col("MON_SALDOPREDIAL_COAC")).otherwise(F.lit(0)))
            .cast(DecimalType(18, 2)).alias("Monto_Saldo_Coact"),
            F.sum(F.when(F.col("VIG_EMISION") == 1, F.col("MON_BASEIMPONIBLE_AFECTO")).otherwise(F.lit(0)))
            .cast(DecimalType(18, 2)).alias("Monto_Base_Imponible"),
            F.max("VIG_EMISION").cast(IntegerType()).alias("VIG_EMISION"),
            F.max("VIG_RECAUD").cast(IntegerType()).alias("VIG_RECAUD"),
            F.max("VIG_SALDO").cast(IntegerType()).alias("VIG_SALDO"),
            F.max("Ind_Outlier_Emision").cast(IntegerType()).alias("Ind_Outlier_Emision"),
            F.max("Ind_Outlier_Saldo").cast(IntegerType()).alias("Ind_Outlier_Saldo"),
            F.max("Ind_Cierre_Reconstruido").cast(IntegerType()).alias("Ind_Cierre_Reconstruido"),
        )
    )

    # Cumplimiento de meta: en cierre anual (MES_ESTADISTICA == 13), recaudación corriente >= año anterior
    rec_corr = F.col("Monto_Recaudado_Ord") + F.col("Monto_Recaudado_Coact")
    w_meta = Window.partitionBy("SEC_EJEC").orderBy("ANO_ESTADISTICA")

    df_cierre_meta = (
        df_agg
        .filter((F.col("MES_ESTADISTICA") == 13) & (F.col("VIG_RECAUD") == 1) & (rec_corr > 0))
        .withColumn("rec_corr", rec_corr)
        .withColumn("rec_prev", F.lag("rec_corr").over(w_meta))
        .withColumn(
            "Ind_Cumplimiento_Meta",
            F.when(F.col("rec_prev").isNull(), F.lit(None).cast(IntegerType()))
            .when(F.col("rec_corr") >= F.col("rec_prev"), F.lit(1))
            .otherwise(F.lit(0)),
        )
        .select("SEC_EJEC", "ANO_ESTADISTICA", "MES_ESTADISTICA", "Ind_Cumplimiento_Meta")
    )

    df_fact = (
        df_agg
        .join(df_cierre_meta, on=["SEC_EJEC", "ANO_ESTADISTICA", "MES_ESTADISTICA"], how="left")
        .withColumn("SK_Municipalidad", F.col("SEC_EJEC").cast(IntegerType()))
        .withColumn(
            "SK_Fecha",
            (F.col("ANO_ESTADISTICA") * 10000 + F.col("MES_ESTADISTICA") * 100 + 1).cast(IntegerType()),
        )
        .select(
            "SK_Municipalidad", "SK_Ubigeo", "SK_Fecha",
            "SEC_EJEC", "ANO_ESTADISTICA", "MES_ESTADISTICA",
            "Monto_Emision_Predial", "Monto_Recaudado_Ord", "Monto_Recaudado_Coact",
            "Monto_Recaud_Anter_Ord", "Monto_Recaud_Anter_Coac",
            "Monto_Saldo_Ord", "Monto_Saldo_Coact", "Monto_Base_Imponible",
            "VIG_EMISION", "VIG_RECAUD", "VIG_SALDO",
            "Ind_Outlier_Emision", "Ind_Outlier_Saldo",
            "Ind_Cierre_Reconstruido", "Ind_Cumplimiento_Meta",
        )
    )

    ruta_dst = f"{GOLD_BASE}/fact_metas_estadistica_atm"
    df_fact.write.mode("overwrite").parquet(ruta_dst)

    filas_escritas = spark.read.parquet(ruta_dst).count()
    t_fin = datetime.now()

    registrar_auditoria(
        spark, id_ejecucion, "GOLD", "03_gold_modelo.py", "gold/fact_metas_estadistica_atm",
        t_ini, t_fin, filas_leidas, filas_escritas, filas_leidas - filas_escritas,
        observaciones="Consolidación bloques vigentes Predial (SK_Fecha AAAA1301 en cierre anual), outliers y cumplimiento de meta",
    )


def construir_fact_cuestionario_atm(
    spark: SparkSession,
    df_dim_entidad: DataFrame,
    df_dim_pregunta: DataFrame,
    id_ejecucion: str,
) -> None:
    """Construye fact_cuestionario_atm (322,675 respuestas) e índice de madurez ATM."""
    t_ini = datetime.now()
    df_resp = spark.read.parquet(f"{SILVER_BASE}/sismepre/rentas_respuestas")
    filas_leidas = df_resp.count()

    df_preg_join = df_dim_pregunta.select(
        "ANO_APLICACION", "PERIODO", "FORMULARIO_ID", "PREGUNTA_ID",
        "SK_Pregunta", "DESCRIPCION",
    )
    df_ent_join = df_dim_entidad.select(
        "SEC_EJEC", "SK_Municipalidad", "SK_Ubigeo",
        F.coalesce(F.col("Software_Tributario"), F.lit(0)).alias("Ent_Sw"),
        F.coalesce(F.col("Catastro_Urbano"), F.lit(0)).alias("Ent_Cat"),
        F.coalesce(F.col("Pasarela_Pagos_Web"), F.lit(0)).alias("Ent_Web"),
        F.coalesce(F.col("P33A"), F.lit(0)).alias("Ent_Coac"),
    )

    df_base = (
        df_resp
        .join(F.broadcast(df_preg_join), on=["ANO_APLICACION", "PERIODO", "FORMULARIO_ID", "PREGUNTA_ID"], how="left")
        .join(F.broadcast(df_ent_join), on="SEC_EJEC", how="left")
    )

    # Detectar respuestas afirmativas de ejecución coactiva y sistema de rentas en el cuestionario
    desc_low = F.lower(F.coalesce(F.col("DESCRIPCION"), F.lit("")))
    resp_up = F.upper(F.trim(F.coalesce(F.col("RESPUESTA_TEXTO"), F.lit(""))))
    es_si = resp_up.isin("SI", "SÍ", "1", "TRUE") | (F.col("RESPUESTA_ENTERO") == 1)

    flag_coac_resp = F.when(
        desc_low.contains("ejecuci") & desc_low.contains("coactiv") & es_si,
        F.lit(1),
    ).otherwise(F.lit(0))

    flag_sist_resp = F.when(
        (desc_low.contains("sistema inform") | desc_low.contains("srtm"))
        & (~desc_low.contains("no tiene sistema"))
        & es_si,
        F.lit(1),
    ).otherwise(F.lit(0))

    df_madurez = (
        df_base
        .withColumn("_coac_r", flag_coac_resp)
        .withColumn("_sist_r", flag_sist_resp)
        .groupBy("SEC_EJEC", "ANO_APLICACION", "PERIODO")
        .agg(
            F.max("_coac_r").alias("_coac_cuest"),
            F.max("_sist_r").alias("_sist_cuest"),
            F.max("Ent_Coac").alias("_ent_coac"),
            F.max("Ent_Sw").alias("_ent_sw"),
            F.max("Ent_Cat").alias("_ent_cat"),
            F.max("Ent_Web").alias("_ent_web"),
        )
        .withColumn("Tiene_Coactivo_Activo", F.greatest(F.col("_coac_cuest"), F.col("_ent_coac")).cast(IntegerType()))
        .withColumn("Tiene_Sistema_Rentas", F.greatest(F.col("_sist_cuest"), F.col("_ent_sw")).cast(IntegerType()))
        .withColumn(
            "Puntaje_Madurez_ATM",
            (
                F.col("Tiene_Sistema_Rentas") * F.lit(35.0)
                + F.col("Tiene_Coactivo_Activo") * F.lit(35.0)
                + F.col("_ent_cat") * F.lit(15.0)
                + F.col("_ent_web") * F.lit(15.0)
            ).cast(DecimalType(5, 2)),
        )
        .select("SEC_EJEC", "ANO_APLICACION", "PERIODO", "Tiene_Coactivo_Activo", "Tiene_Sistema_Rentas", "Puntaje_Madurez_ATM")
    )

    df_fact = (
        df_base
        .join(df_madurez, on=["SEC_EJEC", "ANO_APLICACION", "PERIODO"], how="left")
        .withColumn("SK_Fecha", (F.col("ANO_APLICACION") * 10000 + 1201).cast(IntegerType()))
        .select(
            "SK_Municipalidad", "SK_Ubigeo", "SK_Fecha", "SK_Pregunta",
            "SEC_EJEC", "ANO_APLICACION", "PERIODO", "FORMULARIO_ID", "PREGUNTA_ID", "RESPUESTA_ID",
            "RESPUESTA_TEXTO", "RESPUESTA_DECIMAL", "RESPUESTA_ENTERO", "RESPUESTA_FECHA",
            "Puntaje_Madurez_ATM", "Tiene_Coactivo_Activo", "Tiene_Sistema_Rentas",
        )
    )

    ruta_dst = f"{GOLD_BASE}/fact_cuestionario_atm"
    df_fact.write.mode("overwrite").parquet(ruta_dst)

    filas_escritas = spark.read.parquet(ruta_dst).count()
    assert filas_escritas == 322675, f"fact_cuestionario_atm esperaba 322,675 filas, obtuvo {filas_escritas:,}"

    registrar_auditoria(
        spark, id_ejecucion, "GOLD", "03_gold_modelo.py", "gold/fact_cuestionario_atm",
        t_ini, datetime.now(), filas_leidas, filas_escritas, filas_leidas - filas_escritas,
        observaciones="322,675 respuestas cualitativas ATM con Puntaje_Madurez_ATM, Tiene_Coactivo_Activo y Tiene_Sistema_Rentas",
    )


def validar_integridad_y_cuadre(spark: SparkSession) -> None:
    """Valida 0 huérfanos en las 13 claves foráneas y cuadre contable exacto al céntimo."""
    dim_t = spark.read.parquet(f"{GOLD_BASE}/dim_tiempo").select("SK_Fecha")
    dim_g = spark.read.parquet(f"{GOLD_BASE}/dim_geografia_ubigeo").select("SK_Ubigeo")
    dim_e = spark.read.parquet(f"{GOLD_BASE}/dim_entidad_municipalidad").select("SK_Municipalidad")
    dim_c = spark.read.parquet(f"{GOLD_BASE}/dim_clasificador_ingreso").select("SK_Clasificador")
    dim_r = spark.read.parquet(f"{GOLD_BASE}/dim_fuente_rubro").select("SK_Fuente_Rubro")
    dim_tr = spark.read.parquet(f"{GOLD_BASE}/dim_tipo_recurso").select("SK_Tipo_Recurso")
    dim_p = spark.read.parquet(f"{GOLD_BASE}/dim_formulario_pregunta").select("SK_Pregunta")

    f_ing = spark.read.parquet(f"{GOLD_BASE}/fact_ejecucion_ingresos")
    f_meta = spark.read.parquet(f"{GOLD_BASE}/fact_metas_estadistica_atm")
    f_cuest = spark.read.parquet(f"{GOLD_BASE}/fact_cuestionario_atm")

    checks = [
        ("fact_ejecucion_ingresos.SK_Fecha", f_ing.join(dim_t, "SK_Fecha", "left_anti").count()),
        ("fact_ejecucion_ingresos.SK_Municipalidad", f_ing.join(dim_e, "SK_Municipalidad", "left_anti").count()),
        ("fact_ejecucion_ingresos.SK_Ubigeo", f_ing.join(dim_g, "SK_Ubigeo", "left_anti").count()),
        ("fact_ejecucion_ingresos.SK_Clasificador", f_ing.join(dim_c, "SK_Clasificador", "left_anti").count()),
        ("fact_ejecucion_ingresos.SK_Fuente_Rubro", f_ing.join(dim_r, "SK_Fuente_Rubro", "left_anti").count()),
        ("fact_ejecucion_ingresos.SK_Tipo_Recurso", f_ing.join(dim_tr, "SK_Tipo_Recurso", "left_anti").count()),
        ("fact_metas_estadistica_atm.SK_Fecha", f_meta.join(dim_t, "SK_Fecha", "left_anti").count()),
        ("fact_metas_estadistica_atm.SK_Municipalidad", f_meta.join(dim_e, "SK_Municipalidad", "left_anti").count()),
        ("fact_metas_estadistica_atm.SK_Ubigeo", f_meta.join(dim_g, "SK_Ubigeo", "left_anti").count()),
        ("fact_cuestionario_atm.SK_Fecha", f_cuest.join(dim_t, "SK_Fecha", "left_anti").count()),
        ("fact_cuestionario_atm.SK_Municipalidad", f_cuest.join(dim_e, "SK_Municipalidad", "left_anti").count()),
        ("fact_cuestionario_atm.SK_Ubigeo", f_cuest.join(dim_g, "SK_Ubigeo", "left_anti").count()),
        ("fact_cuestionario_atm.SK_Pregunta", f_cuest.join(dim_p, "SK_Pregunta", "left_anti").count()),
    ]
    huerfanos = [(k, v) for k, v in checks if v > 0]
    assert not huerfanos, f"Llaves huérfanas detectadas en Gold: {huerfanos}"

    tot = f_ing.agg(
        F.sum("MONTO_PIA").cast(DecimalType(22, 2)).alias("pia"),
        F.sum("MONTO_PIM").cast(DecimalType(22, 2)).alias("pim"),
        F.sum("MONTO_RECAUDADO").cast(DecimalType(22, 2)).alias("rec"),
    ).collect()[0]
    assert tot["pia"] == Decimal("327811008619.00"), f"PIA municipal mismatch: {tot['pia']}"
    assert tot["pim"] == Decimal("590726087898.00"), f"PIM municipal mismatch: {tot['pim']}"
    assert tot["rec"] == Decimal("579058362731.05"), f"Recaudado municipal mismatch: {tot['rec']}"
    print("[VALIDACION] 0 huérfanos en 13 FKs | Cuadre Municipal al céntimo OK:", tot.asDict())


def main() -> None:
    id_ejecucion = f"GOLD_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:6]}"
    spark = crear_spark()
    try:
        df_siaf = spark.read.parquet(f"{SILVER_BASE}/siaf")
        df_renamu = spark.read.parquet(f"{SILVER_BASE}/renamu/renamu_2022")
        df_esat = spark.read.parquet(f"{SILVER_BASE}/sismepre/rentas_esat_estadistica_atm")
        df_estado_atm = spark.read.parquet(f"{SILVER_BASE}/sismepre/rentas_entidad_estado")

        construir_dim_tiempo(spark, id_ejecucion)
        construir_dim_geografia(spark, df_siaf, df_renamu, df_esat, id_ejecucion)
        df_dim_ent = construir_dim_entidad(spark, df_siaf, df_renamu, df_estado_atm, id_ejecucion)
        df_dim_clasif = construir_dim_clasificador(spark, df_siaf, id_ejecucion)
        df_dim_rubro = construir_dim_fuente_rubro(spark, df_siaf, id_ejecucion)
        df_dim_trecurso = construir_dim_tipo_recurso(spark, df_siaf, id_ejecucion)
        df_dim_preg = construir_dim_formulario_pregunta(spark, id_ejecucion)

        construir_fact_ejecucion_ingresos(
            spark, df_siaf, df_dim_clasif, df_dim_rubro, df_dim_trecurso, id_ejecucion
        )
        construir_fact_metas_atm(spark, df_esat, id_ejecucion)
        construir_fact_cuestionario_atm(spark, df_dim_ent, df_dim_preg, id_ejecucion)
        validar_integridad_y_cuadre(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
