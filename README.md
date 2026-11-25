# UNMSM FISI — Inteligencia de Negocios | Grupo 3 (PySpark & BI)

Repositorios y entregables oficiales del **Grupo 3** (*Anyel, Gianpool, Angelina, Sebastian, Anthony Yucra y Jack*). Contiene los laboratorios avanzados de **Apache PySpark (Caso H&M — Sesión 13)** y el **Examen Final (Ingresos Municipales MEF + SISMEPRE + RENAMU 2022)** bajo Arquitectura Medallón y tres visores analíticos.

> **Guía maestra operativa (Cierre Sábado 28/11/2026 — 10:00 p. m.):** Consultar [`DOSIER_TECNICO_INTEGRAL_GRUPO3.md`](DOSIER_TECNICO_INTEGRAL_GRUPO3.md) (o su versión [`DOSIER_TECNICO_INTEGRAL_GRUPO3.pdf`](DOSIER_TECNICO_INTEGRAL_GRUPO3.pdf)) para la Matriz RACI, insumos por integrante, qué subir/no subir, escalamiento de dudas y cronograma hora a hora.

---

## Estructura del Repositorio

```text
.
├── compose.yml                             # Docker PySpark + JupyterLab (:8000, :4040) — NO MODIFICAR
├── .gitignore                              # Bloquea Parquets, CSVs, .rar, .pbix y cachés
├── DOSIER_TECNICO_INTEGRAL_GRUPO3.md       # Dosier maestro, RACI, Quickstart, Fichas y Cronograma
├── DOSIER_TECNICO_INTEGRAL_GRUPO3.pdf      # Versión PDF lista para distribuir
├── Labos/
│   └── HyM/                                # Laboratorio 13 — Modelos PySpark sobre H&M
│       ├── referencia_docente/             # Notebooks base del curso + manual_modelos_HYM.pdf
│       ├── lab13/                          # 5 modelos asignados (CLV, LRFM, Churn, Cohortes, ABC/XYZ)
│       ├── bronce/                         # [Google Drive - no en Git]
│       ├── silver/                         # [Google Drive - no en Git] merge_pyspark (obligatorio)
│       └── gold/                           # [Local / Drive - no en Git] salidas analíticas
└── Final/
    └── proyecto_final/                     # Examen Final — MEF Municipalidades
        ├── src/                            # ETL Medallón (01_bronze → 02_silver → 03_gold → 04_gold_marts)
        ├── notebooks/                      # 3 notebooks ejecutados + audit_pipeline.parquet
        ├── assets/                         # peru_departamentos.geojson (25 dptos FIRST_IDDP 01..25)
        ├── docs/                           # Documentación oficial del proyecto
        ├── visores/
        │   ├── powerbi/                    # Tema JSON (#0F2C59) + guía Galaxy Schema (Anyel/Gianpool/Angelina)
        │   ├── streamlit_app/              # Web App RRHH/TIC & Coactiva (:8501) (Sebastian/Jack)
        │   └── dash_app/                   # Mapa coroplético & Autonomía Fiscal (:8050) (Anthony/Gianpool)
        ├── raw/ bronze/ silver/ gold/ gold_marts/ audit/   # [Google Drive / Generado local - no en Git]
```

---

## Enlaces Oficiales de Insumos (Google Drive — Versiones del Grupo 3)

Los datasets pesados, Parquets y `.pbix` **no se suben a GitHub** (`.gitignore` estricto). Descarga únicamente las carpetas de datos requeridas para tu frente desde nuestros Drives oficiales:

- **Laboratorio 13 — Drive Grupo 3 (`HyM.rar` completo con `silver/merge_pyspark` ya procesado):** [Carpeta Drive Lab13](https://drive.google.com/drive/folders/14YSCURa3dyavG58GTntdIRr7-Jy277M1) *(Referencia original del docente solo si se necesita `manual_instalacion_docker.pdf`: [Drive Docente](https://drive.google.com/drive/folders/1JETH3ZMmwM0u50nvTlEhOqZsjNU9QX_h))*.
- **Examen Final — Drive Grupo 3 (`proyecto_final.rar` actualizado `605.5 MB`):** [Carpeta Drive Examen Final](https://drive.google.com/drive/folders/1jJsw07zEPqVG2ouP7SlyjXg3dKkhwx5K) (`proyecto_final.rar`, `INTEGRANTES/`, `RENAMU 2022/`, `Examen Final Documentación`, `Dosier Final Preliminar.pdf`).

> **Regla Anti-Sobrescritura al descomprimir `.rar`:** Clona primero el repositorio desde GitHub y luego extrae desde WinRAR **únicamente** la carpeta de datos de tu rol: `silver/` (`3.11 GB`) para Lab 13; `gold/` (`154.01 MB`) para Power BI; `gold_marts/` (`2.18 MB`) para Streamlit/Dash. **No sobrescribas** archivos `.ipynb`, `.py` o `.md` clonados desde Git.

---

## Quickstart Rápido

### 1. Clonar y crear rama personal (Siempre desde `develop`)
```powershell
git clone https://github.com/jack-jimenes/unmsm-bi-grupo3-pyspark.git
cd unmsm-bi-grupo3-pyspark
git checkout develop
git pull origin develop
git checkout -b feature/<tu-nombre>-<entregable>
```

### 2. PySpark + JupyterLab en Docker (Lab 13 y ETL Final)
```powershell
docker compose up -d
# Abrir: http://127.0.0.1:8000/lab?token=desarrollo
```

### 3. Visores Web Locales (Python 3.11+ en `.venv` independiente)
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# Visor 1: Streamlit (Puerto 8501 — RRHH/TIC y Coactiva)
pip install -r Final\proyecto_final\visores\streamlit_app\requirements.txt
streamlit run Final\proyecto_final\visores\streamlit_app\app.py --server.port 8501

# Visor 2: Dash by Plotly (Puerto 8050 — Mapa Coroplético y Dependencia Fiscal)
pip install -r Final\proyecto_final\visores\dash_app\requirements.txt
python Final\proyecto_final\visores\dash_app\app.py
```

---

## Reglas de Colaboración en GitHub vs. Drive

- **Ramas base:** `main` (entrega congelada — cierre **Sábado 28/11 10:00 p. m.**) y `develop` (integración por Pull Request).
- **SÍ subir a GitHub (`PR` hacia `develop`):** notebooks `.ipynb` **ejecutados con outputs**, scripts `.py`, apps de visores, `requirements.txt`, temas `.json`, `.geojson` y `.md`.
- **NO subir a GitHub (van a Google Drive o locales):** carpetas `raw/`, `bronce/`, `bronze/`, `silver/`, `gold/`, `gold_marts/`, `.parquet`, `.csv`, `.rar` y archivos **`.pbix`** (los `.pbix` van a `Examen Final/INTEGRANTES/` en Drive).
- **Escalamiento:** Git/Docker/PySpark/Medallón → **Jack** | Drive/GeoJSON → **Gianpool** | Power BI → **Anyel** | Cuadre DAX → **Angelina** | Streamlit → **Sebastian** | Dash/Docs → **Anthony Yucra**.
