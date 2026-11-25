# Eje Power BI — 6 Dashboards Oficiales MEF Municipal (Grupo 3)

**Responsables:** **Anyel** (Lead Power BI — D1 y D2), **Gianpool** (D3 y D4), **Angelina** (D5, D6 y QA DAX).

## 1. Paleta Corporativa y Tema Oficial
1. Abrir **Power BI Desktop**.
2. Ir a **Ver → Temas → Buscar temas** y seleccionar [`tema_mef_grupo3.json`](tema_mef_grupo3.json) (color primario `#0F2C59`).

## 2. Conexión a Capa Gold (Solo Municipalidades `NIVEL_GOBIERNO = 'M'`)
Descargar la carpeta `gold/` (~145 MB) desde el Google Drive del Grupo 3 y colocarla en `Final/proyecto_final/gold/` (o conectar mediante Power Query `Parquet.Document`).

### Modelo Galaxy Schema (3 Hechos + 7 Dimensiones)
- **Tablas de Hechos (3):**
  1. `fact_ejecucion_ingresos` (`10,401,713` filas — estrictamente municipal)
  2. `fact_metas_estadistica_atm` (`49,234` filas — Impuesto Predial sin Arbitrios)
  3. `fact_cuestionario_atm` (`322,675` filas — Formularios 2A, 2B, 2C)
- **Dimensiones Conformadas Maestras (3):**
  1. `dim_tiempo` (`6,592` filas; soporta cierres anuales `Mes_Numero = 13`)
  2. `dim_entidad_municipalidad` (`1,964` municipalidades; incluye variables RENAMU `P32_1_T`, `Personal_Total`, `P17_7`, `P17_8`, `P31_3`, `P33A`, `Indice_TIC_Rentas` y `Clasificacion_ATM`)
  3. `dim_geografia_ubigeo` (`1,895` UBIGEOs con `Region_Natural` + miembro `000000`)
- **Dimensiones Específicas de Dominio (4):**
  4. `dim_clasificador_ingreso` (`ES_PREDIAL`)
  5. `dim_fuente_rubro` (`RUBRO` para distinguir Ingresos Propios `08+09` vs. Transferencias `07+18`)
  6. `dim_tipo_recurso`
  7. `dim_formulario_pregunta`

## 3. Distribución de los 6 Dashboards y Reglas DAX Críticas

| Dashboard | Título | Owner | Regla DAX / Filtro Obligatorio |
| :--- | :--- | :--- | :--- |
| **D1** | Macro Presupuesto y Tendencia Histórica (SIAF 2009–2026) | Anyel | Universo 100 % Municipal (`1,964` munis); medir `% Ejecución = DIVIDE(SUM(MONTO_RECAUDADO), SUM(MONTO_PIM))` |
| **D2** | Eficiencia y Metas del Impuesto Predial (Tramos A–G) | Anyel | Filtrar `dim_tiempo[Mes_Numero] = 13`, `VIG_EMISION = 1`/`VIG_RECAUD = 1`/`VIG_SALDO = 1`, `Ind_Outlier_Emision = 0`, `Ind_Outlier_Saldo = 0` |
| **D3** | Capacidad Operativa RRHH & TIC (SIAF/SISMEPRE × RENAMU) | Gianpool | Cruzar `Recaudado_Predial` contra `P32_1_T_Personal_Trib`, `Software_Tributario`, `Catastro_Urbano` e `Indice_TIC_Rentas` |
| **D4** | Autonomía Fiscal y Análisis Territorial | Gianpool | `Ingresos Propios`: `RUBRO IN {"08","09"}` vs. `Transferencias`: `RUBRO IN {"07","18"}` por `Region_Natural`, `Departamento` y `Tipo_Municipalidad` |
| **D5** | Matriz de Control Presupuestal, Extornos y Ranking | Angelina | Transparentar `ES_EXTORNO_NEGATIVO = TRUE()` (`72,194` asientos municipales) sin borrarlos |
| **D6** | Madurez Institucional ATM y Cobranza Coactiva | Angelina | Evaluar `Puntaje_Madurez_ATM`, `Tiene_Coactivo_Activo` (Form. 2B / `P33A`) y brecha por `Clasificacion_ATM` |
