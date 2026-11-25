# Dosier Técnico Integral — Grupo 3 | Inteligencia de Negocios (UNMSM - FISI)

**Docente:** Mg. Pedro Martín Lezama Gonzales  
**Repositorio Oficial GitHub:** `https://github.com/jack-jimenes/unmsm-bi-grupo3-pyspark` *(ramas base `main` y `develop`)*  
**Insumos Google Drive:**
- **Laboratorio 13 (Carpeta del Docente — H&M):** [Lab Arquitectura Medallón - Google Drive](https://drive.google.com/drive/folders/1JETH3ZMmwM0u50nvTlEhOqZsjNU9QX_h) (`HyM.rar`, `manual_instalacion_docker.pdf`)
- **Examen Final (Carpeta del Grupo 3 — MEF):** [Examen Final - Google Drive](https://drive.google.com/drive/folders/1jJsw07zEPqVG2ouP7SlyjXg3dKkhwx5K) (`proyecto_final.rar`, `RENAMU 2022/`, `INTEGRANTES/`, `Examen Final Documentación`, `Dosier Final Preliminar.pdf`)

---

## 1. Resumen Ejecutivo y Matriz RACI

### 1.1 Decisiones Técnicas Aplicadas tras Auditoría
1. **Reorganización del Monorepo Limpio:** Separación física en `Labos/HyM/` (Laboratorio 13) y `Final/proyecto_final/` (Examen Final MEF), preservando `compose.yml` en raíz (`./:/home/jovyan`) y bloqueando todo dato pesado (`*.parquet`, `*.csv`, `*.rar`, `*.pbix`) mediante [`.gitignore`](.gitignore).
2. **Laboratorio 13 Alineado a la Diapositiva Oficial (`Laboratorio: Caso HyM Pyspark`):**
   - Los 5 notebooks en `Labos/HyM/lab13/` parten obligatoriamente de **`Labos/HyM/silver/merge_pyspark`** (`31,788,324` filas, `35` columnas) y ejecutan **100 % en Apache Spark nativo** (solo `toPandas()` sobre tablas resumen ya agregadas para Matplotlib).
   - Se cubren exactamente los 5 modelos solicitados: **CLV**, **LRFM**, **Churn Analysis**, **Modelo de Cohortes** y **ABC / XYZ Segmentation**.
3. **Examen Final — Corrección Crítica de Sesión 13 (`NIVEL_GOBIERNO = 'M'` en Silver):**
   - El pipeline [`02_silver_limpieza.py`](Final/proyecto_final/src/02_silver_limpieza.py) filtra físicamente `NIVEL_GOBIERNO = 'M'`, reduciendo el SIAF desde `16,719,553` filas brutas hasta **`12,735,873` filas en Silver (`-23.83%`, `-3,983,680` filas)** y **`10,401,713` filas en Gold (`-37.79%`)**, con cuadre exacto al céntimo (`Recaudado = S/ 579,058,362,731.05`, diferencia `0.00`) y **0 huérfanos en 13 claves foráneas**.
   - **Galaxy Schema Gold (`154.01 MB`):** **3 tablas de hechos** (`fact_ejecucion_ingresos`, `fact_metas_estadistica_atm`, `fact_cuestionario_atm`), **3 dimensiones conformadas maestras** (`dim_tiempo`, `dim_entidad_municipalidad`, `dim_geografia_ubigeo`) y **4 dimensiones específicas de dominio** (`dim_clasificador_ingreso`, `dim_fuente_rubro`, `dim_tipo_recurso`, `dim_formulario_pregunta`).
   - **Datamarts Livianos para Visores Web (`2.18 MB` en `gold_marts/`):** [`04_gold_marts_visores.py`](Final/proyecto_final/src/04_gold_marts_visores.py) evita levantar Spark en la web app y previene colapsos de RAM (`OOM`), alimentando Streamlit (`:8501`) y Dash (`:8050`, con [`peru_departamentos.geojson`](Final/proyecto_final/assets/peru_departamentos.geojson) mapeado por `FIRST_IDDP = '01'..'25'`) en menos de 1 segundo.

### 1.2 Matriz RACI Rebalanceada (6 Integrantes)

> **Leyenda:** **R** = Responsable (ejecuta) | **A** = Aprobador (rinde cuentas) | **C** = Consultado | **I** = Informado.

| Frente / Entregable Crítico | Anyel | Gianpool | Angelina | Sebastian | Anthony Yucra | Jack (Lead) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **Lab 13 — Modelo 1: CLV (`01_clv_hym_pyspark.ipynb`)** | **R/A** | **R** *(Canal/Edad)* | I | I | I | C |
| **Lab 13 — Modelo 2: LRFM (`02_lrfm_hym_pyspark.ipynb`)** | I | I | I | **R/A** | I | C |
| **Lab 13 — Modelo 3: Churn MLlib (`03_churn_analysis_hym_pyspark.ipynb`)** | I | I | **R/A** | I | I | C |
| **Lab 13 — Modelo 4: Cohortes (`04_cohortes_hym_pyspark.ipynb`)** | I | I | I | I | **R/A** | C |
| **Lab 13 — Modelo 5: ABC/XYZ (`05_abc_xyz_hym_pyspark.ipynb`)** | I | I | I | I | I | **R/A** |
| **Final — Pipeline Medallón `01→04` (`'M'`), Cuadre y Auditoría** | I | C | C | I | I | **R/A** |
| **Final — Eje Power BI (6 Dashboards, Tema `#0F2C59`)** | **R/A** *(D1–D2)* | **R** *(D3–D4)* | **R** *(D5–D6 + QA)* | I | C | I |
| **Final — Eje Streamlit (`:8501`, RRHH/TIC & Coactiva)** | I | I | C | **R/A** | I | **R** *(Co-lead)* |
| **Final — Eje Dash (`:8050`, Mapa Perú & Autonomía Fiscal)** | I | **C** *(QA GeoJSON)* | I | I | **R/A** | C |
| **Gestión — Drive `INTEGRANTES/`, QA GeoJSON & Docs Final** | I | **R** *(Drive/Geo)* | I | I | **R** *(Docs)* | **A** *(GitHub)* |

---

## 2. Guías Rápidas de Onboarding / Quickstart

### 2.1 Clonar Repositorio y Crear Rama Personal
```powershell
git clone https://github.com/jack-jimenes/unmsm-bi-grupo3-pyspark.git
cd unmsm-bi-grupo3-pyspark
git checkout develop
git pull origin develop
git checkout -b feature/<tu-nombre>-<entregable>
```

### 2.2 Entorno Docker + PySpark (Laboratorio 13 y Pipeline Final)
1. Abrir **Docker Desktop** y esperar a que el motor indique `Running`.
2. Descargar [`HyM.rar`](https://drive.google.com/drive/folders/1JETH3ZMmwM0u50nvTlEhOqZsjNU9QX_h) y descomprimir sus carpetas (`bronce/`, `silver/`, `gold/`) dentro de `Labos/HyM/` de forma que exista `Labos/HyM/silver/merge_pyspark/`.
3. Para el Examen Final, descargar `gold/` y `gold_marts/` desde [`Examen Final - Google Drive`](https://drive.google.com/drive/folders/1jJsw07zEPqVG2ouP7SlyjXg3dKkhwx5K) y ubicarlos dentro de `Final/proyecto_final/`.
4. Levantar JupyterLab + PySpark `v3.4.0` sin modificar `compose.yml`:
```powershell
docker compose up -d
# Abrir en navegador: http://127.0.0.1:8000/lab?token=desarrollo
```

### 2.3 Entorno Virtual Python para Streamlit y Dash (Sin Spark en la Web App)
Se ejecuta en **Windows local** (`Python 3.11+`) consumiendo `Final/proyecto_final/gold_marts/` (`2.18 MB`):
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# Para Streamlit (puerto 8501 — Sebastian & Jack)
pip install -r Final\proyecto_final\visores\streamlit_app\requirements.txt
streamlit run Final\proyecto_final\visores\streamlit_app\app.py --server.port 8501

# Para Dash by Plotly (puerto 8050 — Anthony Yucra)
pip install -r Final\proyecto_final\visores\dash_app\requirements.txt
python Final\proyecto_final\visores\dash_app\app.py
```

---

## 3. Fichas de Asignación Individual

### Ficha 1 — Anyel
- **Rol:** Lead Modelado CLV (Lab 13) & Lead Eje Power BI (Examen Final).
- **Insumos exactos a descargar:**
  - Lab 13: `silver/merge_pyspark` desde [Drive Docente](https://drive.google.com/drive/folders/1JETH3ZMmwM0u50nvTlEhOqZsjNU9QX_h).
  - Final: carpeta `gold/` (`154.01 MB`, 10 tablas Parquet) desde [Drive Examen Final](https://drive.google.com/drive/folders/1jJsw07zEPqVG2ouP7SlyjXg3dKkhwx5K).
- **Rama Git:** `feature/anyel-clv-powerbi`
- **Entregables:**
  1. `Labos/HyM/lab13/01_clv_hym_pyspark.ipynb` ejecutado (`../gold/hym_clv`).
  2. Modelo semántico base en Power BI Desktop (`Galaxy Schema` 7 Dims + 3 Hechos) aplicando [`tema_mef_grupo3.json`](Final/proyecto_final/visores/powerbi/tema_mef_grupo3.json) (`#0F2C59`) + **Dashboards D1 (Macro SIAF Municipal) y D2 (Eficiencia Predial Tramos A–G)**. *(El `.pbix` se sube al Drive, nunca a GitHub).*
- **Comandos de prueba:**
```powershell
docker exec hym-transformers-networks2-1 jupyter nbconvert --to notebook --execute --inplace /home/jovyan/Labos/HyM/lab13/01_clv_hym_pyspark.ipynb
```

### Ficha 2 — Gianpool
- **Rol:** Co-owner CLV (Lab 13) + Eje Power BI (D3 & D4) + **QA Datos Compartidos & Drive**.
- **Insumos exactos a descargar:** `silver/merge_pyspark` (Lab 13) y `gold/` + `gold_marts/` (Final) desde los Drives oficiales.
- **Rama Git:** `feature/gianpool-clv-pbi-qa`
- **Entregables:**
  1. Validación cruzada CLV por rango etario y `sales_channel_id` dentro de `Labos/HyM/lab13/01_clv_hym_pyspark.ipynb`.
  2. **Dashboards D3 (RRHH/TIC RENAMU) y D4 (Autonomía Fiscal Territorial)** en Power BI (`#0F2C59`).
  3. Subida y ordenamiento de Parquets Gold (`154.01 MB`) y `gold_marts/` (`2.18 MB`) en la carpeta `INTEGRANTES/` de Drive + validación 1:1 de códigos `'01'..'25'` entre `mart_depto_rubro_anual` y `Final/proyecto_final/assets/peru_departamentos.geojson` para Anthony.
- **Comandos de prueba:**
```powershell
python -c "import json, pandas as pd; g=json.load(open(r'Final\proyecto_final\assets\peru_departamentos.geojson',encoding='utf-8')); ids={f['properties']['FIRST_IDDP'] for f in g['features']}; df=pd.read_parquet(r'Final\proyecto_final\gold_marts\mart_depto_rubro_anual'); print('Match 25/25:', set(df['Cod_Departamento'].unique()) - ids)"
```

### Ficha 3 — Angelina
- **Rol:** Lead Churn Analysis MLlib (Lab 13) + Eje Power BI (D5, D6 y QA DAX).
- **Insumos exactos a descargar:** `silver/merge_pyspark` (Lab 13) y `gold/` (Final).
- **Rama Git:** `feature/angelina-churn-pbi`
- **Entregables:**
  1. `Labos/HyM/lab13/03_churn_analysis_hym_pyspark.ipynb` ejecutado en Docker con corte sin *data leakage* (`Fecha <= 2020-06-22` vs. 90 días posteriores), comparación `LogisticRegression` vs. `RandomForestClassifier` (AUC-ROC y F1) y guardado en `../gold/hym_churn_predicciones`.
  2. **Dashboards D5 (Control de Extornos `72,192` filas y Brecha) y D6 (Madurez ATM y Coactiva)** en Power BI + auditoría de medidas DAX contra [`REPORTE_CUADRE_PYSPARK.md`](Final/proyecto_final/REPORTE_CUADRE_PYSPARK.md) (`S/ 579,058,362,731.05`).
- **Comandos de prueba:**
```powershell
docker exec hym-transformers-networks2-1 jupyter nbconvert --to notebook --execute --inplace /home/jovyan/Labos/HyM/lab13/03_churn_analysis_hym_pyspark.ipynb
```

### Ficha 4 — Sebastian
- **Rol:** Lead Modelo LRFM (Lab 13) + **Lead Eje Streamlit** (Final).
- **Insumos exactos a descargar:** `silver/merge_pyspark` (Lab 13) y `gold_marts/` (`2.18 MB`) en `Final/proyecto_final/gold_marts/`.
- **Rama Git:** `feature/sebastian-lrfm-streamlit`
- **Entregables:**
  1. `Labos/HyM/lab13/02_lrfm_hym_pyspark.ipynb` ejecutado (`../gold/hym_lrfm`).
  2. Evolución y pulido visual de [`Final/proyecto_final/visores/streamlit_app/app.py`](Final/proyecto_final/visores/streamlit_app/app.py) con filtros interactivos y KPIs de RRHH (`P32_1_T`, `Personal_Total`), TIC (`Indice_TIC_Rentas`) y Coactiva (`P33A`, `Tiene_Coactivo_Activo`).
- **Comandos de prueba:**
```powershell
docker exec hym-transformers-networks2-1 jupyter nbconvert --to notebook --execute --inplace /home/jovyan/Labos/HyM/lab13/02_lrfm_hym_pyspark.ipynb
streamlit run Final\proyecto_final\visores\streamlit_app\app.py --server.port 8501
```

### Ficha 5 — Anthony Yucra
- **Rol:** Lead Modelo Cohortes (Lab 13) + **Lead Eje Dash by Plotly** + Actualización Documental Final.
- **Insumos exactos a descargar:** `silver/merge_pyspark` (Lab 13), `gold_marts/` (`mart_depto_rubro_anual`) y [`peru_departamentos.geojson`](Final/proyecto_final/assets/peru_departamentos.geojson) (ya versionado en Git).
- **Rama Git:** `feature/anthony-cohortes-dash`
- **Entregables:**
  1. `Labos/HyM/lab13/04_cohortes_hym_pyspark.ipynb` ejecutado (`../gold/hym_cohortes_retencion`).
  2. Aplicación final [`Final/proyecto_final/visores/dash_app/app.py`](Final/proyecto_final/visores/dash_app/app.py) (`:8050`) con mapa coroplético de los 25 departamentos (`FIRST_IDDP`), gráfico de Ingresos Propios (`08+09`) vs. Transferencias (`07+18`) y evolución de autonomía fiscal.
  3. Actualización del Google Doc `Examen Final Documentación` con las cifras estrictamente municipales (`12,735,873` Silver / `10,401,713` Gold / `S/ 579,058.4 M` / `32` auditorías).
- **Comandos de prueba:**
```powershell
docker exec hym-transformers-networks2-1 jupyter nbconvert --to notebook --execute --inplace /home/jovyan/Labos/HyM/lab13/04_cohortes_hym_pyspark.ipynb
python Final\proyecto_final\visores\dash_app\app.py
```

### Ficha 6 — Jack (Lead / Arquitecto Big Data)
- **Rol:** Lead Arquitecto Big Data, Gobierno GitHub, Modelo ABC/XYZ (Lab 13) & Co-lead Streamlit (Final).
- **Insumos exactos:** Insumos crudos completos + capa Silver/Gold MEF + `silver/merge_pyspark`.
- **Rama Git:** `feature/jack-abcxyz-arquitectura` (revisión de PRs hacia `develop` y `main`).
- **Entregables:**
  1. `Labos/HyM/lab13/05_abc_xyz_hym_pyspark.ipynb` ejecutado (`../gold/hym_abc_xyz`, grilla completa `article_id x mes`).
  2. Pipeline Medallón MEF municipal (`01` a `04`), `REPORTE_CUADRE_PYSPARK.md`, notebooks verificados `1`, `2`, `3` en `Final/proyecto_final/notebooks/` y administración de GitHub.
  3. Soporte de métricas RRHH/TIC y D6 en `Final/proyecto_final/visores/streamlit_app/app.py`.
- **Comandos de prueba:**
```powershell
docker exec hym-transformers-networks2-1 jupyter nbconvert --to notebook --execute --inplace /home/jovyan/Labos/HyM/lab13/05_abc_xyz_hym_pyspark.ipynb
python -m py_compile Final\proyecto_final\src\*.py Final\proyecto_final\visores\streamlit_app\app.py Final\proyecto_final\visores\dash_app\app.py
```

---

## 4. Arquitectura Técnica y Código Base de Inicio

### 4.1 Estándar Obligatorio PySpark Nativo — Laboratorio 13
Todos los notebooks de `Labos/HyM/lab13/` utilizan idéntica configuración de sesión e invocan únicamente transformaciones distribuidas:
```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("HYM_Lab13_Modelo")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .config("spark.sql.shuffle.partitions", "50")
    .config("spark.memory.fraction", "0.8")
    .getOrCreate()
)
spark.sparkContext.setCheckpointDir("/tmp/spark_checkpoint")

# Lectura obligatoria desde Capa Plata unificada (NO desde bronce)
df_silver = spark.read.parquet("../silver/merge_pyspark")
```
> **Prohibición Pandas:** Nunca ejecutar `df_silver.toPandas()` ni `pandas.groupby()`. Solo se permite `.groupBy(...).agg(...).toPandas()` sobre tablas resumen ya agregadas para alimentar gráficos en Matplotlib.

### 4.2 Resumen de los 5 Notebooks Starter de Lab 13
1. [`01_clv_hym_pyspark.ipynb`](Labos/HyM/lab13/01_clv_hym_pyspark.ipynb): Conserva clientes de compra única, calcula `aov`, `vida_dias`, `clv_historico`, `clv_proyectado_12m`, cuartiles con `percent_rank()` y validación por `grupo_edad` y `canal_dominante`.
2. [`02_lrfm_hym_pyspark.ipynb`](Labos/HyM/lab13/02_lrfm_hym_pyspark.ipynb): Extiende RFM incorporando `Length = DATEDIFF(MAX(Fecha), MIN(Fecha))` junto a `Recency`, `Frequency` y `Monetary`, asignando quintiles `1..5` y 5 segmentos accionables.
3. [`03_churn_analysis_hym_pyspark.ipynb`](Labos/HyM/lab13/03_churn_analysis_hym_pyspark.ipynb): Corte estricto sin *data leakage* (`<= 2020-06-22` vs. 90 días futuros), 8 variables conductuales y pipeline MLlib (`LogisticRegression` vs. `RandomForestClassifier`).
4. [`04_cohortes_hym_pyspark.ipynb`](Labos/HyM/lab13/04_cohortes_hym_pyspark.ipynb): Cálculo de `cohorte_mes` e `indice_mes` en Spark SQL, matriz de `% tasa_retencion_pct` y `gasto_medio_activo` con visualización Heatmap.
5. [`05_abc_xyz_hym_pyspark.ipynb`](Labos/HyM/lab13/05_abc_xyz_hym_pyspark.ipynb): Resuelve ausencia de `quantity` (1 fila = 1 unidad); **ABC** sobre ingreso acumulado (`SUM() OVER`) y **XYZ** sobre `CV = std / mean` **sobre la grilla completa `article_id x mes`** (imputando `0` en meses sin venta).

### 4.3 Patrón Base de Visores Web (Consumo Directo de `gold_marts/`)
- **Streamlit (`visores/streamlit_app/app.py`, puerto `8501`):** Utiliza `@st.cache_data` para leer `mart_municipal_anual` (`1.89 MB`), `mart_atm_tramos_anual` (`0.05 MB`) y `mart_madurez_coactiva` (`0.11 MB`) con paleta corporativa `#0F2C59`.
- **Dash by Plotly (`visores/dash_app/app.py`, puerto `8050`):** Cruza `mart_depto_rubro_anual` (`0.13 MB`, códigos `'01'..'25'`) con `assets/peru_departamentos.geojson` mediante `featureidkey="properties.FIRST_IDDP"` garantizando cobertura exacta de los 25 departamentos del Perú (incluyendo Callao `'07'` separado de Lima `'15'`).

---

## 5. Cronograma Operativo

*(Fechas estructuradas desde el **25/11/2026**; ajustar fechas finales `[Según cronograma oficial de cátedra]` si el docente fija un cierre distinto).*

| Hito | Fecha Objetivo | Responsables | Entregables Verificables |
| :--- | :--- | :--- | :--- |
| **H0 — Arranque Repo & Insumos** | **25/11/2026** | Jack, Gianpool | Push inicial en `unmsm-bi-grupo3-pyspark`, upload de `gold/` (`154.01 MB`) y `gold_marts/` (`2.18 MB`) a Drive `INTEGRANTES/` y `pull` del equipo. |
| **H1 — Ejecución Notebooks Lab 13** | **26/11/2026** | Anyel, Gianpool, Sebastian, Angelina, Anthony, Jack | Los 5 `.ipynb` de `Labos/HyM/lab13/` ejecutados en Docker sin Pandas y con outputs guardados + `PR` a `develop`. |
| **H2 — Validación Conjunta Lab 13 & GeoJSON** | **27/11/2026** | Gianpool, Jack, Angelina | Verificación de salidas `gold/hym_*` y match 25/25 de `peru_departamentos.geojson`. |
| **H3 — Visores Alpha (PBI + Streamlit + Dash)** | **28/11/2026** | Anyel, Gianpool, Angelina, Sebastian, Anthony | Dashboards D1–D6 conectados en Power BI (`#0F2C59`), Streamlit en `:8501` y mapa Dash en `:8050` operativos. |
| **H4 — Congelamiento, QA Final & Docs** | **29/11/2026** | Anthony, Angelina, Jack | Cuadre DAX vs. `S/ 579,058.4 M`, Google Doc actualizado, merge `develop -> main` y ensayo de exposición. |
| **H5 — Entrega y Sustentación Final** | **30/11/2026** *(ajustar según aula)* | Todo el Grupo 3 | Presentación de negocio primero (KPIs + Wireframes + 3 Visores) seguida de arquitectura Medallón y Lab 13. |
