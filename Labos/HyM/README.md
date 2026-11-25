# Laboratorio 13 — Modelos Analíticos en PySpark (Caso H&M)

Subproyecto de Inteligencia de Negocios enfocado en el diseño de **5 modelos analíticos 100 % en Apache Spark nativo** partiendo obligatoriamente de la **Capa Plata (`silver/merge_pyspark`)**.

## Insumos y Referencia
- **Descarga de datos:** [Carpeta Drive del Docente (`HyM.rar` + `manual_instalacion_docker.pdf`)](https://drive.google.com/drive/folders/1JETH3ZMmwM0u50nvTlEhOqZsjNU9QX_h).
- Colocar `bronce/`, `silver/` y `gold/` dentro de `Labos/HyM/`. El insumo oficial de entrada para todos los notebooks de `lab13/` es `../silver/merge_pyspark` (`31,788,324` filas, `35` columnas).
- **Regla técnica:** Todas las transformaciones, ventanas y agregaciones se ejecutan en Spark (`pyspark.sql` / `pyspark.ml`). El uso de `toPandas()` solo se admite sobre tablas resumen finales o matrices agregadas listas para graficar.

## Modelos Requeridos (`lab13/`)

| Notebook | Modelo Oficial | Responsables | Salida en Capa Gold |
| :--- | :--- | :--- | :--- |
| [`lab13/01_clv_hym_pyspark.ipynb`](lab13/01_clv_hym_pyspark.ipynb) | **CLV** (Customer Lifetime Value) | Anyel (Lead) & Gianpool (Validación canal/edad) | `gold/hym_clv` |
| [`lab13/02_lrfm_hym_pyspark.ipynb`](lab13/02_lrfm_hym_pyspark.ipynb) | **LRFM** (Length, Recency, Frequency, Monetary) | Sebastian | `gold/hym_lrfm` |
| [`lab13/03_churn_analysis_hym_pyspark.ipynb`](lab13/03_churn_analysis_hym_pyspark.ipynb) | **Churn Analysis** (Análisis de abandono) | Angelina | `gold/hym_churn_predicciones` |
| [`lab13/04_cohortes_hym_pyspark.ipynb`](lab13/04_cohortes_hym_pyspark.ipynb) | **Modelo de Cohortes** (Retención y gasto mensual) | Anthony Yucra | `gold/hym_cohortes_retencion` |
| [`lab13/05_abc_xyz_hym_pyspark.ipynb`](lab13/05_abc_xyz_hym_pyspark.ipynb) | **ABC / XYZ Segmentation** (Pareto + CV grilla completa) | Jack | `gold/hym_abc_xyz` |

## Notebooks de Referencia del Docente (`referencia_docente/`)
Incluye `1_Caso_H_M_Pyspark.ipynb` (construcción de `silver/merge_pyspark`), `2_kmeans_HYM.ipynb`, `3_RFM_HYM.ipynb`, `4_bisecting_kmeans_HYM.ipynb`, `5_weighted_fuzzy_rfm_HYM.ipynb` y [`manual_modelos_HYM.pdf`](referencia_docente/manual_modelos_HYM.pdf).
