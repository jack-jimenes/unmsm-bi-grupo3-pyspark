# UNMSM FISI — Inteligencia de Negocios | Grupo 3 (PySpark & BI)

Repositorio oficial del **Grupo 3** (*Anyel, Gianpool, Angelina, Sebastian, Anthony Yucra y Jack*). Contiene los laboratorios avanzados de **Apache PySpark (Caso H&M — Sesión 13)** y el **Examen Final (Ingresos Municipales MEF + SISMEPRE + RENAMU 2022)** bajo Arquitectura Medallón y tres visores analíticos.

> **Documentación operativa completa:** Consultar [`DOSIER_TECNICO_INTEGRAL_GRUPO3.md`](DOSIER_TECNICO_INTEGRAL_GRUPO3.md) para la Matriz RACI, fichas individuales por integrante, ramas Git, comandos de ejecución y cronograma.

---

## Estructura del Repositorio

```text
.
├── compose.yml                             # Docker PySpark + JupyterLab (:8000, :4040) — NO MODIFICAR
├── .gitignore                              # Bloquea Parquets, CSVs, .rar, .pbix y cachés
├── DOSIER_TECNICO_INTEGRAL_GRUPO3.md       # Dosier maestro, RACI, Quickstart y Fichas
├── Labos/
│   └── HyM/                                # Laboratorio 13 — Modelos PySpark sobre H&M
│       ├── referencia_docente/             # Notebooks base del curso + manual_modelos_HYM.pdf
│       ├── lab13/                          # 5 modelos asignados (CLV, LRFM, Churn, Cohortes, ABC/XYZ)
│       ├── bronce/                         # [Google Drive - no en Git]
│       ├── silver/                         # [Google Drive - no en Git] merge_pyspark
│       └── gold/                           # [Local - no en Git] salidas analíticas
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

## Enlaces Oficiales de Insumos (Google Drive)

Los datasets pesados y Parquets **no se suben a GitHub** (`.gitignore` estricto). Descárgalos desde los repositorios oficiales:

- **Laboratorio 13 (Carpeta del Docente — H&M):** [Lab Arquitectura Medallón - Google Drive](https://drive.google.com/drive/folders/1JETH3ZMmwM0u50nvTlEhOqZsjNU9QX_h) (`HyM.rar` 530.6 MB, `manual_instalacion_docker.pdf` 2.1 MB)
- **Examen Final (Carpeta del Grupo 3 — MEF):** [Examen Final - Google Drive](https://drive.google.com/drive/folders/1jJsw07zEPqVG2ouP7SlyjXg3dKkhwx5K) (`proyecto_final.rar` 649.4 MB, `INTEGRANTES/`, `RENAMU 2022/`, `Examen Final Documentación`, `Dosier Final Preliminar.pdf`)

---

## Quickstart Rápido

### 1. PySpark + JupyterLab en Docker (Lab 13 y ETL Final)
```powershell
docker compose up -d
# Abrir: http://127.0.0.1:8000/lab?token=desarrollo
```

### 2. Visores Web Locales (Python 3.11+ en `venv` independiente)
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

## Reglas de Colaboración en GitHub

- **Ramas base:** `main` (producción / entregas finales) y `develop` (integración del equipo).
- **Ramas por integrante (`feature/...`):** crea tu rama desde `develop` antes de programar (`git checkout -b feature/<nombre>-<modulo>`), nunca hagas `push` directo a `main` y verifica `git status` para asegurar que no estás subiendo archivos `.parquet`, `.csv`, `.rar` ni `.pbix`.
