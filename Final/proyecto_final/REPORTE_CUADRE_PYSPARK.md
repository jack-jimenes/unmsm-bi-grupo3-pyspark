# Reporte de Cuadre y Validación del Pipeline Medallón Municipal en PySpark

**Proyecto:** Examen Final — Inteligencia de Negocios (MEF SIAF 2009–2026, SISMEPRE/ATM y RENAMU 2022)  
**Alcance Estratégico (Corrección Sesión 13 — Prof. Pedro Lezama):** Estrictamente segmentado a **Gobiernos Locales (`NIVEL_GOBIERNO = 'M'`)** desde la Capa Silver (`02_silver_limpieza.py`).  
**Motor de Ejecución:** Apache Spark `v3.4.0` en contenedor Docker `hym-transformers-networks2-1` (`spark-submit` con `driver.memory=4g`, `executor.memory=4g`, `shuffle.partitions=50`, `adaptive.enabled=true`, `adaptive.coalescePartitions.enabled=true`, checkpoint en `/tmp/spark_checkpoint`).  
**Scripts del Pipeline (`Final/proyecto_final/src/`):**
- [`audit_logger.py`](src/audit_logger.py)
- [`01_bronze_ingesta.py`](src/01_bronze_ingesta.py)
- [`02_silver_limpieza.py`](src/02_silver_limpieza.py)
- [`03_gold_modelo.py`](src/03_gold_modelo.py)
- [`04_gold_marts_visores.py`](src/04_gold_marts_visores.py)

---

## 1. Cuadre Contable al Céntimo (Dominio SIAF — Segmentación Estricta `NIVEL_GOBIERNO = 'M'`)

Todos los montos monetarios se castearon a `DecimalType(18, 2)` en Silver y se agregaron en `DecimalType(22, 2)` para verificación exacta al céntimo. En `02_silver_limpieza.py` se excluyeron los cortes superpuestos (`2025-Ingreso-Mensual.csv` y `2026-Ingreso-Mensual.csv`) **y se filtró físicamente `NIVEL_GOBIERNO = 'M'`**, eliminando **`3,983,680` filas** (**`-23.83%`** respecto a las `16,719,553` filas de Bronze Bruto: `1,534,239` por snapshots redundantes + `2,449,441` netas de Gobierno Nacional `'E'` y Gobiernos Regionales `'R'`). En Gold (`fact_ejecucion_ingresos`), se descartaron además las `2,334,146` filas municipales con montos en cero (`ES_FILA_SIN_MONTO`) y se agruparon por grano mediante `SUM()` reduciendo `14` filas duplicadas (`-37.79%` acumulado vs. bruto).

| Capa / Etapa | Filas | Variación vs. Bronze Bruto | `MONTO_PIA` (S/) | `MONTO_PIM` (S/) | `MONTO_RECAUDADO` (S/) | Diferencia vs. Base `'M'` |
| :--- | ---: | :---: | ---: | ---: | ---: | :---: |
| **Bronze Bruto Total (20 CSV, E+R+M)** | 16,719,553 | Base bruta (`100%`) | 1,165,690,206,489.00 | 1,661,810,691,699.00 | 1,825,254,431,926.16 | +3,983,680 filas (`E`/`R` + snapshots) |
| **Bronze Neto Municipal (18 CSV, `M`)** | 12,735,873 | **-23.83%** | 327,811,008,619.00 | 590,726,087,898.00 | 579,058,362,731.05 | Base `'M'` (0.00) |
| **Silver Municipal (`silver/siaf`)** | 12,735,873 | **-23.83%** | 327,811,008,619.00 | 590,726,087,898.00 | 579,058,362,731.05 | **0.00** (Cuadre exacto) |
| **Gold Municipal (`fact_ejecucion_ingresos`)** | 10,401,713 | **-37.79%** | 327,811,008,619.00 | 590,726,087,898.00 | 579,058,362,731.05 | **0.00** (Cuadre exacto) |

---

## 2. Auditoría de Asientos Contables Negativos (`NIVEL_GOBIERNO = 'M'`)

Los asientos negativos municipales (extornos, anulaciones y devoluciones tributarias) se conservaron íntegramente bajo el flag `ES_EXTORNO_NEGATIVO = (MONTO_RECAUDADO < 0)` para respetar el principio contable de percibido neto del SIAF.

| Segmento / Métrica | Cantidad de Registros | Suma Monetaria Exacta (S/) | Valor Mínimo Observado (S/) | Observación |
| :--- | ---: | ---: | ---: | :--- |
| **Silver Municipal — `MONTO_RECAUDADO < 0`** | 72,194 | -8,099,958,601.69 | -88,369,790.06 | 100 % Gobiernos Locales (`'M'`) |
| **Gold (`fact_ejecucion_ingresos`) — Agrupado por grano** | 72,192 | -8,099,958,601.69 | — | Diferencia exacta: **S/ 0.00** |

### Validación Complementaria de Reglas de Calidad y Galaxy Schema
- **SISMEPRE / `rentas_respuestas.csv`:** Se recuperaron los **322,675** registros íntegros en Bronze, Silver y Gold (`fact_cuestionario_atm`) mediante tokenización posicional sobre delimitadores `","`, remediando las comillas dobles internas sin escapar.
- **SISMEPRE / ESAT (`fact_metas_estadistica_atm`):** Se excluyeron `38,749` filas de Arbitrios por `TITULO`, quedando `110,549` filas Predial + `1,189` filas de cierre anual reconstruido (`Ind_Cierre_Reconstruido = 1`; `111,738` filas en Silver y `49,234` filas consolidadas por bloque vigente en Gold). En cierre anual (`MES = 13`): `Ind_Outlier_Emision = 1` marcó `2` atípicos y `Ind_Outlier_Saldo = 1` marcó `3` atípicos.
- **RENAMU + Imputación Post-2022 (`dim_entidad_municipalidad`):** Total de **`1,964` municipalidades** (`1,944` censadas en RENAMU 2022 y `20` distritos nuevos post-censo 2022 imputados por mediana departamental/tipología; **0 entidades `'E'` ni `'R'`**). En Silver RENAMU se podaron `1,368` columnas brutas a **`17` variables operativas/censales clave**.
- **Integridad Referencial en Gold:** **0 llaves huérfanas** validadas automáticamente en las **13 claves foráneas** (`SK_Fecha`, `SK_Municipalidad`, `SK_Ubigeo`, `SK_Clasificador`, `SK_Fuente_Rubro`, `SK_Tipo_Recurso` y `SK_Pregunta`) de las tres tablas de hechos.

---

## 3. Contenido Completo de `audit/audit_pipeline.parquet`

Ruta en contenedor: `/home/jovyan/Final/proyecto_final/audit/audit_pipeline.parquet` (**`32` registros totales**: `9 BRONZE`, `9 SILVER`, `10 GOLD` y `4 GOLD_MARTS`, todos en estado **`EXITOSO`**).

| capa | proceso_script | fuente_tabla | inicio (UTC) | duracion_seg | filas_leidas | filas_escritas | filas_descartadas | estado |
| :--- | :--- | :--- | :--- | ---: | ---: | ---: | ---: | :--- |
| BRONZE | 01_bronze_ingesta.py | bronze/siaf | 2026-11-16 06:46:08 | 168.38 | 16,719,553 | 16,719,553 | 0 | EXITOSO |
| BRONZE | 01_bronze_ingesta.py | bronze/sismepre/rentas_ano_aplicacion | 2026-11-16 06:49:02 | 5.45 | 27 | 27 | 0 | EXITOSO |
| BRONZE | 01_bronze_ingesta.py | bronze/sismepre/rentas_entidad_estado | 2026-11-16 06:49:11 | 6.20 | 20,391 | 20,391 | 0 | EXITOSO |
| BRONZE | 01_bronze_ingesta.py | bronze/sismepre/rentas_esat_estadistica_atm | 2026-11-16 06:49:21 | 14.31 | 149,298 | 149,298 | 0 | EXITOSO |
| BRONZE | 01_bronze_ingesta.py | bronze/sismepre/rentas_estadistica | 2026-11-16 06:49:38 | 4.69 | 233 | 233 | 0 | EXITOSO |
| BRONZE | 01_bronze_ingesta.py | bronze/sismepre/rentas_formulario | 2026-11-16 06:49:46 | 6.25 | 98 | 98 | 0 | EXITOSO |
| BRONZE | 01_bronze_ingesta.py | bronze/sismepre/rentas_preguntas | 2026-11-16 06:49:56 | 5.87 | 836 | 836 | 0 | EXITOSO |
| BRONZE | 01_bronze_ingesta.py | bronze/sismepre/rentas_respuestas | 2026-11-16 06:50:05 | 6.21 | 322,675 | 322,675 | 0 | EXITOSO |
| BRONZE | 01_bronze_ingesta.py | bronze/renamu/renamu_2022 | 2026-11-16 06:50:14 | 7.01 | 1,874 | 1,874 | 0 | EXITOSO |
| SILVER | 02_silver_limpieza.py | silver/siaf | 2026-11-25 05:18:25 | 289.09 | 16,719,553 | 12,735,873 | 3,983,680 | EXITOSO |
| SILVER | 02_silver_limpieza.py | silver/sismepre/rentas_ano_aplicacion | 2026-11-25 05:23:18 | 6.15 | 27 | 27 | 0 | EXITOSO |
| SILVER | 02_silver_limpieza.py | silver/sismepre/rentas_entidad_estado | 2026-11-25 05:23:26 | 6.43 | 20,391 | 20,391 | 0 | EXITOSO |
| SILVER | 02_silver_limpieza.py | silver/sismepre/rentas_formulario | 2026-11-25 05:23:35 | 7.23 | 98 | 98 | 0 | EXITOSO |
| SILVER | 02_silver_limpieza.py | silver/sismepre/rentas_estadistica | 2026-11-25 05:23:46 | 5.91 | 233 | 233 | 0 | EXITOSO |
| SILVER | 02_silver_limpieza.py | silver/sismepre/rentas_preguntas | 2026-11-25 05:23:55 | 6.58 | 836 | 836 | 0 | EXITOSO |
| SILVER | 02_silver_limpieza.py | silver/sismepre/rentas_respuestas | 2026-11-25 05:24:04 | 9.31 | 322,675 | 322,675 | 0 | EXITOSO |
| SILVER | 02_silver_limpieza.py | silver/sismepre/rentas_esat_estadistica_atm | 2026-11-25 05:24:16 | 57.44 | 149,298 | 111,738 | 38,749 | EXITOSO |
| SILVER | 02_silver_limpieza.py | silver/renamu/renamu_2022 | 2026-11-25 05:25:16 | 6.19 | 1,874 | 1,874 | 0 | EXITOSO |
| GOLD | 03_gold_modelo.py | gold/dim_tiempo | 2026-11-25 05:25:38 | 4.91 | 6,592 | 6,592 | 0 | EXITOSO |
| GOLD | 03_gold_modelo.py | gold/dim_geografia_ubigeo | 2026-11-25 05:25:47 | 26.03 | 1,895 | 1,895 | 0 | EXITOSO |
| GOLD | 03_gold_modelo.py | gold/dim_entidad_municipalidad | 2026-11-25 05:26:14 | 21.31 | 1,964 | 1,964 | 0 | EXITOSO |
| GOLD | 03_gold_modelo.py | gold/dim_clasificador_ingreso | 2026-11-25 05:26:39 | 13.09 | 432 | 432 | 0 | EXITOSO |
| GOLD | 03_gold_modelo.py | gold/dim_fuente_rubro | 2026-11-25 05:26:54 | 12.41 | 6 | 6 | 0 | EXITOSO |
| GOLD | 03_gold_modelo.py | gold/dim_tipo_recurso | 2026-11-25 05:27:09 | 10.56 | 145 | 145 | 0 | EXITOSO |
| GOLD | 03_gold_modelo.py | gold/dim_formulario_pregunta | 2026-11-25 05:27:23 | 4.34 | 836 | 836 | 0 | EXITOSO |
| GOLD | 03_gold_modelo.py | gold/fact_ejecucion_ingresos | 2026-11-25 05:27:30 | 38.55 | 12,735,873 | 10,401,713 | 2,334,160 | EXITOSO |
| GOLD | 03_gold_modelo.py | gold/fact_metas_estadistica_atm | 2026-11-25 05:28:11 | 7.42 | 111,738 | 49,234 | 62,504 | EXITOSO |
| GOLD | 03_gold_modelo.py | gold/fact_cuestionario_atm | 2026-11-25 05:28:21 | 6.57 | 322,675 | 322,675 | 0 | EXITOSO |
| GOLD_MARTS | 04_gold_marts_visores.py | gold_marts/mart_municipal_anual | 2026-11-25 05:29:15 | 14.25 | 10,401,713 | 34,128 | 10,367,585 | EXITOSO |
| GOLD_MARTS | 04_gold_marts_visores.py | gold_marts/mart_depto_rubro_anual | 2026-11-25 05:29:36 | 9.19 | 10,401,713 | 5,363 | 10,396,350 | EXITOSO |
| GOLD_MARTS | 04_gold_marts_visores.py | gold_marts/mart_atm_tramos_anual | 2026-11-25 05:29:50 | 3.95 | 10,363 | 1,643 | 8,720 | EXITOSO |
| GOLD_MARTS | 04_gold_marts_visores.py | gold_marts/mart_madurez_coactiva | 2026-11-25 05:29:59 | 4.65 | 322,675 | 3,073 | 319,602 | EXITOSO |

---

## 4. Resumen de Tablas Parquet en `gold/` (Power BI) y `gold_marts/` (Streamlit & Dash)

| Tabla | Capa / Rol | Ruta Contenedor | Filas | Columnas | Tamaño Parquet |
| :--- | :--- | :--- | ---: | ---: | ---: |
| `dim_tiempo` | Gold — Dim. Conformada Maestra | `/home/jovyan/Final/proyecto_final/gold/dim_tiempo` | 6,592 | 9 | 0.08 MB (82,250 B) |
| `dim_entidad_municipalidad` | Gold — Dim. Conformada Maestra | `/home/jovyan/Final/proyecto_final/gold/dim_entidad_municipalidad` | 1,964 | 39 | 0.15 MB (152,249 B) |
| `dim_geografia_ubigeo` | Gold — Dim. Conformada Maestra | `/home/jovyan/Final/proyecto_final/gold/dim_geografia_ubigeo` | 1,895 | 8 | 0.03 MB (35,545 B) |
| `dim_clasificador_ingreso` | Gold — Dim. Específica SIAF | `/home/jovyan/Final/proyecto_final/gold/dim_clasificador_ingreso` | 432 | 13 | 0.02 MB (20,698 B) |
| `dim_fuente_rubro` | Gold — Dim. Específica SIAF | `/home/jovyan/Final/proyecto_final/gold/dim_fuente_rubro` | 6 | 5 | 0.00 MB (2,289 B) |
| `dim_tipo_recurso` | Gold — Dim. Específica SIAF | `/home/jovyan/Final/proyecto_final/gold/dim_tipo_recurso` | 145 | 5 | 0.01 MB (6,122 B) |
| `dim_formulario_pregunta` | Gold — Dim. Específica ATM | `/home/jovyan/Final/proyecto_final/gold/dim_formulario_pregunta` | 836 | 19 | 0.04 MB (36,875 B) |
| `fact_ejecucion_ingresos` | Gold — Tabla de Hechos 1 (`'M'`) | `/home/jovyan/Final/proyecto_final/gold/fact_ejecucion_ingresos` | 10,401,713 | 17 | 151.42 MB (158,777,526 B) |
| `fact_metas_estadistica_atm` | Gold — Tabla de Hechos 2 (Predial) | `/home/jovyan/Final/proyecto_final/gold/fact_metas_estadistica_atm` | 49,234 | 21 | 0.99 MB (1,033,554 B) |
| `fact_cuestionario_atm` | Gold — Tabla de Hechos 3 (2A/2B/2C) | `/home/jovyan/Final/proyecto_final/gold/fact_cuestionario_atm` | 322,675 | 17 | 1.28 MB (1,342,383 B) |
| **Total Capa Gold** | **10 tablas Parquet** | `/home/jovyan/Final/proyecto_final/gold/` | **10,785,492** | — | **154.01 MB** |
| `mart_municipal_anual` | Gold Marts — Visor Streamlit | `/home/jovyan/Final/proyecto_final/gold_marts/mart_municipal_anual` | 34,128 | 29 | 1.89 MB (1,983,710 B) |
| `mart_depto_rubro_anual` | Gold Marts — Mapa Dash | `/home/jovyan/Final/proyecto_final/gold_marts/mart_depto_rubro_anual` | 5,363 | 12 | 0.13 MB (132,484 B) |
| `mart_madurez_coactiva` | Gold Marts — Streamlit D6 | `/home/jovyan/Final/proyecto_final/gold_marts/mart_madurez_coactiva` | 3,073 | 23 | 0.11 MB (114,876 B) |
| `mart_atm_tramos_anual` | Gold Marts — Streamlit Tramos | `/home/jovyan/Final/proyecto_final/gold_marts/mart_atm_tramos_anual` | 1,643 | 13 | 0.05 MB (57,181 B) |
| **Total `gold_marts/`** | **4 datamarts compactos** | `/home/jovyan/Final/proyecto_final/gold_marts/` | **44,207** | — | **2.18 MB** |
