# Examen Final — Observatorio Fiscal de Municipalidades del Perú (MEF + SISMEPRE + RENAMU)

Pipeline Medallón completo en **Apache PySpark** que integra la ejecución de ingresos del MEF (SIAF 2009–2026), el seguimiento del Impuesto Predial y cuestionarios institucionales (SISMEPRE/ATM) y el Censo Nacional de Municipalidades (RENAMU 2022), segmentado estrictamente a **Gobiernos Locales (`NIVEL_GOBIERNO = 'M'`)** desde la capa Silver.

## Arquitectura del Pipeline (`src/`)

1. **`src/audit_logger.py`**: Logger transaccional en modo `append` sobre `audit/audit_pipeline.parquet`.
2. **`src/01_bronze_ingesta.py`**: Ingesta cruda sin `inferSchema` (esquemas `StructType` en `StringType()`), recuperación de las 9 filas malformadas de `rentas_respuestas.csv` y lectura BOM-safe de `Base_RENAMU_2022_f.csv` (`1,368` columnas).
3. **`src/02_silver_limpieza.py`**: Exclusión de snapshots redundantes `2025/2026-Ingreso-Mensual.csv` **+ filtro estricto `NIVEL_GOBIERNO = 'M'` (`12,735,873` filas municipales, reducción de `-23.83%` respecto a `16,719,553` brutas)**, preservación de `72,194` extornos negativos, exclusión de Arbitrios por `TITULO` y poda de RENAMU de `1,368 → 17` variables de negocio.
4. **`src/03_gold_modelo.py`**: Constelación de Hechos (**3 tablas de hechos** + **7 dimensiones**: 3 conformadas maestras + 4 específicas), cuadre contable al céntimo (`0.00` diferencia) y **0 llaves huérfanas** en 13 FKs.
5. **`src/04_gold_marts_visores.py`**: Generación de 4 datamarts agregados (`< 5 MB` totales) en `gold_marts/` para alimentar Streamlit y Dash sin JVM.

## Notebooks de Verificación (`notebooks/`) y Reporte
- [`notebooks/1_Caso_MEF_Medallon_Ingesta_y_Silver_PySpark.ipynb`](notebooks/1_Caso_MEF_Medallon_Ingesta_y_Silver_PySpark.ipynb)
- [`notebooks/2_Modelo_Constelacion_Gold_y_Cuadre_PySpark.ipynb`](notebooks/2_Modelo_Constelacion_Gold_y_Cuadre_PySpark.ipynb)
- [`notebooks/3_Analitica_SparkSQL_Indicadores_y_Auditoria.ipynb`](notebooks/3_Analitica_SparkSQL_Indicadores_y_Auditoria.ipynb)
- [`REPORTE_CUADRE_PYSPARK.md`](REPORTE_CUADRE_PYSPARK.md)

## Insumos Compartidos (Google Drive)
Descargar desde [Examen Final - Google Drive](https://drive.google.com/drive/folders/1jJsw07zEPqVG2ouP7SlyjXg3dKkhwx5K).
