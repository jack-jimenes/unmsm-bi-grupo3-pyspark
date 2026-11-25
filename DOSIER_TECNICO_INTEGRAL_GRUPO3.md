# Dosier Técnico Integral — Grupo 3 | Inteligencia de Negocios (UNMSM - FISI)

**Docente:** Mg. Pedro Martín Lezama Gonzales  
**Repositorio Oficial GitHub:** `https://github.com/jack-jimenes/unmsm-bi-grupo3-pyspark` *(ramas base `main` y `develop`)*  
**Insumos Oficiales Google Drive (Versiones Actualizadas por Jack):**
- **Laboratorio 13 — H&M Completo (Grupo 3):** [Carpeta Drive Lab13 (`HyM.rar` con `bronce/`, `silver/merge_pyspark/`, `gold/`, `lab13/`)](https://drive.google.com/drive/folders/14YSCURa3dyavG58GTntdIRr7-Jy277M1) *(Referencia original docente para `manual_instalacion_docker.pdf`: [Drive Docente](https://drive.google.com/drive/folders/1JETH3ZMmwM0u50nvTlEhOqZsjNU9QX_h))*
- **Examen Final — MEF Municipalidades (Grupo 3):** [Carpeta Drive Examen Final (`proyecto_final.rar` `605.5 MB`, `INTEGRANTES/`, `RENAMU 2022/`, `Examen Final Documentación`, `Dosier Final Preliminar.pdf`)](https://drive.google.com/drive/folders/1jJsw07zEPqVG2ouP7SlyjXg3dKkhwx5K)

---

## 1. Resumen Ejecutivo y Matriz RACI

### 1.1 Decisiones Técnicas Aplicadas tras Auditoría
1. **Reorganización del Monorepo Limpio:** Separación física en `Labos/HyM/` (Laboratorio 13) y `Final/proyecto_final/` (Examen Final MEF), preservando `compose.yml` en raíz (`./:/home/jovyan`) y bloqueando todo dato pesado (`*.parquet`, `*.csv`, `*.rar`, `*.pbix`) mediante [`.gitignore`](.gitignore).
2. **Laboratorio 13 Alineado a la Diapositiva Oficial (`Laboratorio: Caso HyM Pyspark`):**
   - Los 5 notebooks en `Labos/HyM/lab13/` parten obligatoriamente de **`Labos/HyM/silver/merge_pyspark`** (`31,788,324` filas, `35` columnas, ya pre-procesado dentro de nuestro `HyM.rar`) y ejecutan **100 % en Apache Spark nativo** (solo `toPandas()` sobre tablas resumen ya agregadas para Matplotlib).
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

## 2. Guías Rápidas de Onboarding y Cómo Avanzar

### 2.1 Flujo Paso a Paso para Avanzar Sin Conflictos
1. **Clonar el repositorio y trabajar siempre desde `develop` con rama personal:**
```powershell
git clone https://github.com/jack-jimenes/unmsm-bi-grupo3-pyspark.git
cd unmsm-bi-grupo3-pyspark
git checkout develop
git pull origin develop
git checkout -b feature/<tu-nombre>-<entregable>
```
2. **Descargar e inyectar SOLO los insumos de datos de tu frente (Regla Anti-Sobrescritura):**
   - **El código siempre manda desde GitHub** (`Labos/HyM/lab13/`, `Final/proyecto_final/src/`, `visores/`, `assets/`, `README.md`).
   - Los archivos `.rar` de Google Drive (`HyM.rar` y `proyecto_final.rar` de `8.83 GB` descomprimidos) contienen copias completas del entorno de Jack. Al abrirlos con WinRAR, **extrae únicamente las carpetas de datos que necesitas** (NO sobrescribas archivos `.py`, `.ipynb` ni `.md` que bajaste con `git clone`):
     - **Para Lab 13 (Todos):** de [`HyM.rar` (Drive Lab13)](https://drive.google.com/drive/folders/14YSCURa3dyavG58GTntdIRr7-Jy277M1) extrae la carpeta `silver/` (`3.11 GB`, contiene `merge_pyspark/`) dentro de `Labos/HyM/`. *(Opcional: extrae `gold/` de `120.81 MB` si deseas comparar con KMeans/RFM del docente; NO necesitas `bronce/`).*
     - **Para Power BI (Anyel, Gianpool, Angelina):** de [`proyecto_final.rar` (Drive Final)](https://drive.google.com/drive/folders/1jJsw07zEPqVG2ouP7SlyjXg3dKkhwx5K) abre `proyecto_final/` y extrae **únicamente `gold/`** (`154.01 MB`) en `Final/proyecto_final/gold/`. *(Ahorras ~8.6 GB porque no requieres `raw/`, `bronze/` ni `silver/`).*
     - **Para Visores Streamlit / Dash (Sebastian, Anthony):** de [`proyecto_final.rar` (Drive Final)](https://drive.google.com/drive/folders/1jJsw07zEPqVG2ouP7SlyjXg3dKkhwx5K) abre `proyecto_final/` y extrae **únicamente `gold_marts/`** (`2.18 MB`) en `Final/proyecto_final/gold_marts/`.
     - **Para Auditoría ETL Completa (Solo Jack):** conserva `raw/`, `bronze/`, `silver/`, `gold/`, `gold_marts/` y `audit/`.
3. **Ejecutar, validar y guardar outputs:** corre tu notebook en Docker (guardando tablas y gráficos en celdas) o prueba tu dashboard/visor web localmente.
4. **Subir avances diarios antes de cerrar cada hito:**
   - **Código / Notebooks `.ipynb` / Apps:** `git status` -> `git add <tus archivos>` -> `git commit -m "..."` -> `git push origin feature/...` -> **Pull Request hacia `develop`** (nunca a `main`).
   - **Pesados / `.pbix` / Docs:** subir al **Google Drive** correspondiente según la Tabla 4.1.

### 2.2 Entorno Docker + PySpark (Laboratorio 13 y Pipeline Final)
1. Abrir **Docker Desktop** y esperar a que el motor indique `Running`.
2. Verificar que exista `Labos/HyM/silver/merge_pyspark/_SUCCESS` (descomprimido desde [`HyM.rar` en Drive Lab13](https://drive.google.com/drive/folders/14YSCURa3dyavG58GTntdIRr7-Jy277M1)).
3. Levantar JupyterLab + PySpark `v3.4.0` sin modificar `compose.yml`:
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

## 3. Fichas de Asignación Individual (Insumos, Entregables y Destino)

### Ficha 1 — Anyel
- **Rol:** Lead Modelado CLV (Lab 13) & **Lead Eje Power BI** (Examen Final).
- **Insumos exactos:**
  - **Lab 13:** `silver/` (`merge_pyspark`, `3.11 GB`) desde `HyM.rar` en [Drive Lab13](https://drive.google.com/drive/folders/14YSCURa3dyavG58GTntdIRr7-Jy277M1).
  - **Final:** únicamente carpeta `gold/` (`154.01 MB`, 10 Parquets) desde `proyecto_final.rar` en [Drive Final](https://drive.google.com/drive/folders/1jJsw07zEPqVG2ouP7SlyjXg3dKkhwx5K) + [`tema_mef_grupo3.json`](Final/proyecto_final/visores/powerbi/tema_mef_grupo3.json) (en Git).
- **Rama Git:** `feature/anyel-clv-powerbi`
- **Entregables y Dónde Subirlos:**
  1. `Labos/HyM/lab13/01_clv_hym_pyspark.ipynb` ejecutado con outputs y gráficos (`../gold/hym_clv`). **-> Subir `.ipynb` a GitHub (PR a `develop`).** *(Coordinar turno de edición en este notebook con Gianpool).*
  2. Modelo semántico base en Power BI Desktop (`Galaxy Schema` 7 Dims + 3 Hechos) + tema `#0F2C59` + **Dashboards D1 (Macro SIAF Municipal) y D2 (Eficiencia Predial Tramos A–G)**. **-> Subir `.pbix` base y final a Drive `Examen Final/INTEGRANTES/Anyel/` (NUNCA a GitHub).**
- **A quién preguntar:** **Jack** (PySpark/Docker/Git) | **Angelina** (cuadre DAX).
- **Comando de prueba:**
```powershell
docker exec hym-transformers-networks2-1 jupyter nbconvert --to notebook --execute --inplace /home/jovyan/Labos/HyM/lab13/01_clv_hym_pyspark.ipynb
```

### Ficha 2 — Gianpool
- **Rol:** Co-owner CLV (Lab 13) + Eje Power BI (D3 & D4) + **QA Datos Compartidos & Drive**.
- **Insumos exactos:** `silver/merge_pyspark` (`HyM.rar` en [Drive Lab13](https://drive.google.com/drive/folders/14YSCURa3dyavG58GTntdIRr7-Jy277M1)) y `gold/` (`154.01 MB`) + `gold_marts/` (`2.18 MB`) desde `proyecto_final.rar` en [Drive Final](https://drive.google.com/drive/folders/1jJsw07zEPqVG2ouP7SlyjXg3dKkhwx5K).
- **Rama Git:** `feature/gianpool-clv-pbi-qa`
- **Entregables y Dónde Subirlos:**
  1. Validación cruzada CLV por rango etario y `sales_channel_id` dentro de `Labos/HyM/lab13/01_clv_hym_pyspark.ipynb` (una vez Anyel suba el cálculo base). **-> Subir actualización `.ipynb` a GitHub (PR a `develop`).**
  2. **Dashboards D3 (RRHH/TIC RENAMU) y D4 (Autonomía Fiscal Territorial)** sobre el `.pbix` base de Anyel (`#0F2C59`). **-> Subir `.pbix` con D3–D4 a Drive `Examen Final/INTEGRANTES/Gianpool/` (NUNCA a GitHub).**
  3. Organización de carpetas sueltas `gold/` (`154.01 MB`) y `gold_marts/` (`2.18 MB`) en `Examen Final/INTEGRANTES/` de Drive + validación `25/25` de códigos `FIRST_IDDP` entre `mart_depto_rubro_anual` y `peru_departamentos.geojson` para Anthony. **-> Confirmar por grupo y subir checklist a Drive.**
- **A quién preguntar:** **Anyel** (estructura del `.pbix` base) | **Jack** (rutas Parquet/Spark) | **Anthony** (consumo en Dash).
- **Comando de prueba:**
```powershell
python -c "import json, pandas as pd; g=json.load(open(r'Final\proyecto_final\assets\peru_departamentos.geojson',encoding='utf-8')); ids={f['properties']['FIRST_IDDP'] for f in g['features']}; df=pd.read_parquet(r'Final\proyecto_final\gold_marts\mart_depto_rubro_anual'); print('Match 25/25:', set(df['Cod_Departamento'].unique()) - ids)"
```

### Ficha 3 — Angelina
- **Rol:** Lead Churn Analysis MLlib (Lab 13) + Eje Power BI (**D5, D6 y QA Contable DAX**).
- **Insumos exactos:** `silver/merge_pyspark` (`HyM.rar` en [Drive Lab13](https://drive.google.com/drive/folders/14YSCURa3dyavG58GTntdIRr7-Jy277M1)) y `gold/` (`154.01 MB` desde `proyecto_final.rar` en [Drive Final](https://drive.google.com/drive/folders/1jJsw07zEPqVG2ouP7SlyjXg3dKkhwx5K)) + [`REPORTE_CUADRE_PYSPARK.md`](Final/proyecto_final/REPORTE_CUADRE_PYSPARK.md).
- **Rama Git:** `feature/angelina-churn-pbi`
- **Entregables y Dónde Subirlos:**
  1. `Labos/HyM/lab13/03_churn_analysis_hym_pyspark.ipynb` ejecutado con corte sin *data leakage* (`Fecha <= 2020-06-22` vs. 90 días posteriores), comparación `LogisticRegression` vs. `RandomForestClassifier` (AUC-ROC y F1) y salida `../gold/hym_churn_predicciones`. **-> Subir `.ipynb` a GitHub (PR a `develop`).**
  2. **Dashboards D5 (Control de Extornos `72,192` filas y Brecha) y D6 (Madurez ATM y Coactiva)** + auditoría final de todas las medidas DAX (`D1–D6`) contra `S/ 579,058,362,731.05`. **-> Subir `.pbix` consolidado final a Drive `Examen Final/INTEGRANTES/PowerBI_Final/` (NUNCA a GitHub).**
- **A quién preguntar:** **Jack** (pipeline MLlib sin Pandas / cifras `REPORTE_CUADRE_PYSPARK.md`) | **Anyel & Gianpool** (fusión D1–D4 antes de D5–D6).
- **Comando de prueba:**
```powershell
docker exec hym-transformers-networks2-1 jupyter nbconvert --to notebook --execute --inplace /home/jovyan/Labos/HyM/lab13/03_churn_analysis_hym_pyspark.ipynb
```

### Ficha 4 — Sebastian
- **Rol:** Lead Modelo LRFM (Lab 13) + **Lead Eje Streamlit** (Final).
- **Insumos exactos:** `silver/merge_pyspark` (`HyM.rar` en [Drive Lab13](https://drive.google.com/drive/folders/14YSCURa3dyavG58GTntdIRr7-Jy277M1)) y únicamente `gold_marts/` (`2.18 MB` desde `proyecto_final.rar` en [Drive Final](https://drive.google.com/drive/folders/1jJsw07zEPqVG2ouP7SlyjXg3dKkhwx5K)).
- **Rama Git:** `feature/sebastian-lrfm-streamlit`
- **Entregables y Dónde Subirlos:**
  1. `Labos/HyM/lab13/02_lrfm_hym_pyspark.ipynb` ejecutado con quintiles `1..5` y 5 segmentos (`../gold/hym_lrfm`). **-> Subir `.ipynb` a GitHub (PR a `develop`).**
  2. Evolución y pulido visual de [`Final/proyecto_final/visores/streamlit_app/app.py`](Final/proyecto_final/visores/streamlit_app/app.py) con filtros interactivos y KPIs de RRHH (`P32_1_T`, `Personal_Total`), TIC (`Indice_TIC_Rentas`) y Coactiva (`P33A`, `Tiene_Coactivo_Activo`). **-> Subir `app.py` (y `requirements.txt` si cambia) a GitHub (PR a `develop`).**
- **A quién preguntar:** **Jack** (Co-lead de Streamlit, arquitectura de `gold_marts/` y ventanas LRFM en Spark) | **Gianpool** (validación de `gold_marts/`).
- **Comandos de prueba:**
```powershell
docker exec hym-transformers-networks2-1 jupyter nbconvert --to notebook --execute --inplace /home/jovyan/Labos/HyM/lab13/02_lrfm_hym_pyspark.ipynb
streamlit run Final\proyecto_final\visores\streamlit_app\app.py --server.port 8501
```

### Ficha 5 — Anthony Yucra
- **Rol:** Lead Modelo Cohortes (Lab 13) + **Lead Eje Dash by Plotly** + **Actualización Documental Final**.
- **Insumos exactos:** `silver/merge_pyspark` (`HyM.rar` en [Drive Lab13](https://drive.google.com/drive/folders/14YSCURa3dyavG58GTntdIRr7-Jy277M1)), únicamente `gold_marts/` (`2.18 MB` de `proyecto_final.rar` en [Drive Final](https://drive.google.com/drive/folders/1jJsw07zEPqVG2ouP7SlyjXg3dKkhwx5K)) y [`peru_departamentos.geojson`](Final/proyecto_final/assets/peru_departamentos.geojson) (ya versionado en Git).
- **Rama Git:** `feature/anthony-cohortes-dash`
- **Entregables y Dónde Subirlos:**
  1. `Labos/HyM/lab13/04_cohortes_hym_pyspark.ipynb` ejecutado con heatmap de retención (`../gold/hym_cohortes_retencion`). **-> Subir `.ipynb` a GitHub (PR a `develop`).**
  2. Aplicación final [`Final/proyecto_final/visores/dash_app/app.py`](Final/proyecto_final/visores/dash_app/app.py) (`:8050`) con mapa coroplético de los 25 departamentos (`FIRST_IDDP`), gráfico de Ingresos Propios (`08+09`) vs. Transferencias (`07+18`) y evolución de autonomía fiscal. **-> Subir `app.py` a GitHub (PR a `develop`).**
  3. Actualización del Google Doc **`Examen Final Documentación`** con las cifras estrictamente municipales (`12,735,873` Silver / `10,401,713` Gold / `S/ 579,058.4 M` / `32` auditorías) y capturas de los 3 visores. **-> Editar directo en Google Drive (`Examen Final/`).**
- **A quién preguntar:** **Gianpool** (cruce `Cod_Departamento` vs `FIRST_IDDP` y `gold_marts/`) | **Jack** (cifras oficiales de `REPORTE_CUADRE_PYSPARK.md` y Cohortes en Spark SQL) | **Angelina/Anyel** (capturas Power BI para el Doc).
- **Comandos de prueba:**
```powershell
docker exec hym-transformers-networks2-1 jupyter nbconvert --to notebook --execute --inplace /home/jovyan/Labos/HyM/lab13/04_cohortes_hym_pyspark.ipynb
python Final\proyecto_final\visores\dash_app\app.py
```

### Ficha 6 — Jack (Lead / Arquitecto Big Data)
- **Rol:** Lead Arquitecto Big Data, **Gobierno GitHub (`develop`/`main`)**, Modelo ABC/XYZ (Lab 13) & **Co-lead Streamlit**.
- **Insumos exactos:** Repositorio local completo (`Labos/HyM/` + `Final/proyecto_final/` completos con `raw/`, `bronze/`, `silver/`, `gold/`, `gold_marts/`).
- **Rama Git:** `feature/jack-abcxyz-arquitectura` (+ revisión/merge de PRs hacia `develop` y liberación final en `main`).
- **Entregables y Dónde Subirlos:**
  1. `Labos/HyM/lab13/05_abc_xyz_hym_pyspark.ipynb` ejecutado (`../gold/hym_abc_xyz`, grilla completa `article_id x mes`). **-> Subir `.ipynb` a GitHub (`develop -> main`).**
  2. Pipeline Medallón MEF municipal (`01` a `04`), `REPORTE_CUADRE_PYSPARK.md`, notebooks verificados `1`, `2`, `3` en `Final/proyecto_final/notebooks/` y mantenimiento de `HyM.rar` / `proyecto_final.rar` en Drive. **-> GitHub + Drive.**
  3. Soporte de métricas RRHH/TIC y D6 en `Final/proyecto_final/visores/streamlit_app/app.py` e integración final del monorepo para la demo en vivo. **-> GitHub (`develop -> main`).**
- **Comandos de prueba:**
```powershell
docker exec hym-transformers-networks2-1 jupyter nbconvert --to notebook --execute --inplace /home/jovyan/Labos/HyM/lab13/05_abc_xyz_hym_pyspark.ipynb
python -m py_compile Final\proyecto_final\src\*.py Final\proyecto_final\visores\streamlit_app\app.py Final\proyecto_final\visores\dash_app\app.py
```

---

## 4. Reglas GitHub vs. Drive, Escalamiento y Cierre del Equipo

### 4.1 Qué SÍ Subir y Qué NO Subir a GitHub (`.gitignore` estricto)

| Qué SÍ va a GitHub (PR hacia `develop`) | Qué NO va a GitHub | Dónde va lo que NO se sube a GitHub |
| :--- | :--- | :--- |
| Notebooks `.ipynb` **ejecutados con sus outputs y gráficos** (`Labos/HyM/lab13/*.ipynb`, `Final/proyecto_final/notebooks/*.ipynb`) | Carpetas de datos pesados (`raw/`, `bronce/`, `bronze/`, `silver/`, `gold/`, `gold_marts/`, `audit/`) | Insumos base ya están en `HyM.rar` ([Drive Lab13](https://drive.google.com/drive/folders/14YSCURa3dyavG58GTntdIRr7-Jy277M1)) y `proyecto_final.rar` ([Drive Final](https://drive.google.com/drive/folders/1jJsw07zEPqVG2ouP7SlyjXg3dKkhwx5K)) |
| Código Python (`Final/proyecto_final/src/*.py`, `streamlit_app/app.py`, `dash_app/app.py`, `requirements.txt`) | Archivos `.parquet`, `.csv`, `.crc`, `_SUCCESS`, `.rar`, `.zip` | Salidas nuevas de Lab 13 (`gold/hym_*`): respaldo opcional en carpeta **`Lab13/`** de Drive (el `.ipynb` ya guarda tablas y gráficos) |
| Assets livianos (`peru_departamentos.geojson`, `tema_mef_grupo3.json`), `README.md`, `REPORTE_CUADRE_PYSPARK.md` y este Dosier | Archivos Power BI (`.pbix`, `.pbit`) y carpetas `.venv/`, `__pycache__/`, `.ipynb_checkpoints/` | Archivos **`.pbix`** de Power BI -> subir en **`Examen Final/INTEGRANTES/`** (Google Drive) |
| **Reglas de rama:** solo commits en `feature/...` y Pull Request a `develop` | **PROHIBIDO:** `push` directo a `main`, editar `compose.yml` o sobrescribir `.ipynb` de otro compañero | Documento oficial Word/GDoc -> editar en **`Examen Final/Examen Final Documentación`** (Google Drive) |

### 4.2 Directorio de Escalamiento ("¿A quién preguntar para poder avanzar?")

| Tipo de Duda o Bloqueo | Preguntar a (Primario) | Respaldo |
| :--- | :--- | :--- |
| Git / ramas `feature`, errores de `merge`/PR o Docker no levanta | **Jack** | — |
| Descargas `.rar` de Drive, rutas `gold/` o `gold_marts/`, validación GeoJSON `'01'..'25'` | **Gianpool** | **Jack** |
| Errores PySpark nativo en Lab 13 (`merge_pyspark`, ventanas, MLlib sin Pandas) | **Jack** | Owner del modelo paralelo (Anyel/Sebastian/Angelina/Anthony) |
| Modelo Galaxy Schema, tema `#0F2C59` y diseño visual en Power BI (`D1–D6`) | **Anyel** | **Gianpool** |
| Cuadre contable de medidas DAX vs `S/ 579,058,362,731.05` y extornos `72,192` | **Angelina** | **Jack** |
| Visor Streamlit (`:8501`) y métricas RRHH/TIC/Coactiva | **Sebastian** | **Jack** |
| Visor Dash (`:8050`), mapa coroplético Perú y Google Doc `Examen Final Documentación` | **Anthony Yucra** | **Gianpool** *(mapa)* / **Jack** *(cifras)* |

### 4.3 ¿Al final todos deben tener todos los archivos o solo algunos?
- **Código, Notebooks con outputs, Visores y Docs (`git pull origin main`):** **SÍ, TODOS los 6 integrantes** deben hacer `git pull origin main` el **sábado 28/11 entre 21:00 y 22:00** para tener la foto final limpia del repositorio.
- **Carpetas pesadas de datos (`raw/`, `bronze/`, `silver/`, `bronce/`):** **NO es necesario que todos tengan todo.** Son más de `12 GB` combinados que nadie necesita completos salvo **Jack**:
  - Quien solo desarrolla **Streamlit o Dash** únicamente conserva `gold_marts/` (`2.18 MB`) para correr las web apps.
  - Quien trabaja **Power BI** conserva `gold/` (`154.01 MB`) y el `.pbix` final.
  - Para **Lab 13**, solo hace falta conservar `Labos/HyM/silver/merge_pyspark` (`3.11 GB`) mientras se ejecuta el `.ipynb`.
- **Equipo de Respaldo para Sustentación:** Para la exposición en vivo, **Jack** (PC principal) y **2 respaldos designados** (**Anyel** con el `.pbix` final + `gold/`, y **Sebastian o Anthony** con `.venv` + `gold_marts/`) deben tener listos los visores ejecutables y el repositorio `main` actualizado; los 5 `.ipynb` de Lab 13 y los 3 `.ipynb` del Final se muestran ya ejecutados con sus outputs en GitHub/JupyterLab sin obligar a re-procesar `31.7 M` de filas en plena sala.

---

## 5. Arquitectura Técnica y Código Base de Inicio

### 5.1 Estándar Obligatorio PySpark Nativo — Laboratorio 13
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

### 5.2 Resumen de los 5 Notebooks Starter de Lab 13
1. [`01_clv_hym_pyspark.ipynb`](Labos/HyM/lab13/01_clv_hym_pyspark.ipynb): Conserva clientes de compra única, calcula `aov`, `vida_dias`, `clv_historico`, `clv_proyectado_12m`, cuartiles con `percent_rank()` y validación por `grupo_edad` y `canal_dominante`.
2. [`02_lrfm_hym_pyspark.ipynb`](Labos/HyM/lab13/02_lrfm_hym_pyspark.ipynb): Extiende RFM incorporando `Length = DATEDIFF(MAX(Fecha), MIN(Fecha))` junto a `Recency`, `Frequency` y `Monetary`, asignando quintiles `1..5` y 5 segmentos accionables.
3. [`03_churn_analysis_hym_pyspark.ipynb`](Labos/HyM/lab13/03_churn_analysis_hym_pyspark.ipynb): Corte estricto sin *data leakage* (`<= 2020-06-22` vs. 90 días futuros), 8 variables conductuales y pipeline MLlib (`LogisticRegression` vs. `RandomForestClassifier`).
4. [`04_cohortes_hym_pyspark.ipynb`](Labos/HyM/lab13/04_cohortes_hym_pyspark.ipynb): Cálculo de `cohorte_mes` e `indice_mes` en Spark SQL, matriz de `% tasa_retencion_pct` y `gasto_medio_activo` con visualización Heatmap.
5. [`05_abc_xyz_hym_pyspark.ipynb`](Labos/HyM/lab13/05_abc_xyz_hym_pyspark.ipynb): Resuelve ausencia de `quantity` (1 fila = 1 unidad); **ABC** sobre ingreso acumulado (`SUM() OVER`) y **XYZ** sobre `CV = std / mean` **sobre la grilla completa `article_id x mes`** (imputando `0` en meses sin venta).

### 5.3 Patrón Base de Visores Web (Consumo Directo de `gold_marts/`)
- **Streamlit (`visores/streamlit_app/app.py`, puerto `8501`):** Utiliza `@st.cache_data` para leer `mart_municipal_anual` (`1.89 MB`), `mart_atm_tramos_anual` (`0.05 MB`) y `mart_madurez_coactiva` (`0.11 MB`) con paleta corporativa `#0F2C59`.
- **Dash by Plotly (`visores/dash_app/app.py`, puerto `8050`):** Cruza `mart_depto_rubro_anual` (`0.13 MB`, códigos `'01'..'25'`) con `assets/peru_departamentos.geojson` mediante `featureidkey="properties.FIRST_IDDP"` garantizando cobertura exacta de los 25 departamentos del Perú (incluyendo Callao `'07'` separado de Lima `'15'`).

---

## 6. Cronograma Operativo — Cierre Total Sábado 28/11/2026 (10:00 p. m.)

*(Calendario exacto desde hoy **Miércoles 25/11/2026** hasta la fecha límite de entrega y congelamiento: **Sábado 28/11/2026 a las 22:00 h / 10:00 p. m.**)*

| Hito | Ventana Exacta | Responsables | Entregables Verificables y Criterio de Aceptación |
| :--- | :--- | :--- | :--- |
| **H0 — Arranque Repo & Descarga Selectiva** | **Mié 25/11/2026** *(Hoy — hasta 23:59)* | **Jack**, **Gianpool** + Todo el equipo | Subida final de `HyM.rar` en [Drive Lab13](https://drive.google.com/drive/folders/14YSCURa3dyavG58GTntdIRr7-Jy277M1); cada integrante clona `develop`, crea su rama `feature/...`, levanta Docker/`:8000` y `.venv` local sin pisar archivos Git. |
| **H1 — Ejecución 5 Notebooks Lab 13 (Sin Pandas)** | **Jue 26/11/2026** *(08:00 – 22:00)* | **Anyel, Gianpool, Sebastian, Angelina, Anthony, Jack** | Los 5 `.ipynb` de `Labos/HyM/lab13/` ejecutados en Docker con outputs/gráficos guardados, salidas `../gold/hym_*` generadas y **PR abierto hacia `develop`**. *(Anyel sube CLV base antes de las 17:00 para que Gianpool agregue validación canal/edad hasta las 22:00)*. |
| **H2 — Visores Alpha (Power BI + Streamlit + Dash) & QA Lab 13** | **Vie 27/11/2026** *(08:00 – 20:00)* | **Anyel, Gianpool, Angelina, Sebastian, Anthony** | Anyel entrega `.pbix` base (`D1–D2` + Galaxy Schema) en Drive a las **13:00**; Gianpool suma `D3–D4` hasta las **20:00** y valida match `25/25` GeoJSON; Sebastian (`:8501`) y Anthony (`:8050`) dejan visores web funcionales con PR a `develop`. Jack aprueba PRs de Lab 13. |
| **H3 — Cierre Power BI (`D5–D6`), Cuadre DAX y Docs** | **Vie 27/11 20:00 → Sáb 28/11 13:00** | **Angelina, Anthony, Jack** | Angelina integra `D5–D6` en el `.pbix` único y certifica cuadre DAX vs `S/ 579,058,362,731.05` + `72,192` extornos en Gold; Anthony actualiza el Google Doc `Examen Final Documentación` con cifras municipales y capturas. |
| **H4 — Congelamiento de Código (`develop -> main`) & Respaldo Drive** | **Sáb 28/11/2026** *(13:00 – 19:00)* | **Jack** + **Gianpool** + Todo el equipo | Todos los PRs fusionados en `develop`; QA técnico final (`py_compile`, outputs `.ipynb`, Streamlit `:8501`, Dash `:8050`); **merge `develop -> main`**; `.pbix` final subido a Drive `INTEGRANTES/PowerBI_Final/`. |
| **H5 — Cierre Definitivo, Pull del Equipo & Ensayo** | **Sáb 28/11/2026** *(19:00 – **22:00 h Cierre Total**)* | **Todo el Grupo 3** | Todos ejecutan `git pull origin main`; **Jack**, **Anyel** y **Sebastian/Anthony** verifican entorno de demo en vivo; ensayo breve de exposición (**negocio/KPIs/visores primero**, luego arquitectura Medallón y Lab 13). **Cierre oficial: 10:00 p. m.** |
