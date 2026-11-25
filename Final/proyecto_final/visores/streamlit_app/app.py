"""Visor Streamlit: Capacidad Operativa RRHH/TIC, Madurez ATM y Cobranza Coactiva Municipal."""

from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(
    page_title="MEF Municipal | RRHH, TIC y Madurez Coactiva",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="expanded",
)

PRIMARY = "#0F2C59"
ACCENT = "#1E4E8C"
TEAL = "#1B7F5B"
AMBER = "#D97706"
RED = "#B91C1C"

st.markdown(
    f"""
    <style>
        .main-banner {{
            background: linear-gradient(90deg, {PRIMARY} 0%, {ACCENT} 100%);
            color: #FFFFFF;
            padding: 1.2rem 1.6rem;
            border-radius: 10px;
            margin-bottom: 1rem;
        }}
        .kpi-card {{
            background-color: #FFFFFF;
            border: 1px solid #E2E8F0;
            border-left: 5px solid {PRIMARY};
            border-radius: 8px;
            padding: 0.9rem 1rem;
            box-shadow: 0 1px 3px rgba(15, 44, 89, 0.08);
        }}
        .kpi-label {{
            font-size: 0.80rem;
            color: #475569;
            font-weight: 600;
            text-transform: uppercase;
        }}
        .kpi-val {{
            font-size: 1.55rem;
            color: {PRIMARY};
            font-weight: 700;
            margin-top: 0.2rem;
        }}
    </style>
    """,
    unsafe_allow_html=True,
)

MARTS_DIR = Path(__file__).resolve().parents[2] / "gold_marts"


@st.cache_data
def cargar_marts() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Carga datamarts agregados Gold (<5 MB) directamente con PyArrow/Pandas."""
    m_muni = pd.read_parquet(MARTS_DIR / "mart_municipal_anual")
    m_tramo = pd.read_parquet(MARTS_DIR / "mart_atm_tramos_anual")
    m_mad = pd.read_parquet(MARTS_DIR / "mart_madurez_coactiva")
    return m_muni, m_tramo, m_mad


try:
    df_muni, df_tramo, df_mad = cargar_marts()
except Exception as exc:
    st.error(
        f"No se pudieron leer los archivos en `{MARTS_DIR}`. "
        f"Ejecuta primero `04_gold_marts_visores.py` en Docker o descarga `gold_marts/` desde Drive.\n\nDetalle: {exc}"
    )
    st.stop()

st.markdown(
    """
    <div class="main-banner">
        <h2 style="margin:0;">🏛️ Observatorio Fiscal Municipal — RRHH, TIC y Cobranza Coactiva</h2>
        <p style="margin:0.25rem 0 0 0; opacity:0.9;">
            Cruce SIAF-MEF × RENAMU 2022 × SISMEPRE/ATM | Universo estricto: Gobiernos Locales (NIVEL_GOBIERNO = 'M')
        </p>
    </div>
    """,
    unsafe_allow_html=True,
)

# ======================== SIDEBAR FILTROS ========================
st.sidebar.header("Filtros Municipales")
anios = sorted(df_muni["Anio"].dropna().unique().tolist())
rango_anios = st.sidebar.slider(
    "Rango de Años (SIAF)",
    min_value=int(min(anios)),
    max_value=int(max(anios)),
    value=(max(2016, int(min(anios))), int(max(anios))),
)
regiones = sorted([r for r in df_muni["Region_Natural"].dropna().unique() if r != "Sin Dato"])
regiones_sel = st.sidebar.multiselect("Región Natural", regiones, default=regiones)

tipos = sorted(df_muni["Tipo_Municipalidad"].dropna().unique().tolist())
tipos_sel = st.sidebar.multiselect("Tipo de Municipalidad", tipos, default=tipos)

tramos = sorted(df_muni["Clasificacion_ATM"].fillna("Sin Tramo").unique().tolist())
tramos_sel = st.sidebar.multiselect("Clasificación ATM (Tramo)", tramos, default=tramos)

df_m_f = df_muni[
    (df_muni["Anio"].between(rango_anios[0], rango_anios[1]))
    & (df_muni["Region_Natural"].isin(regiones_sel))
    & (df_muni["Tipo_Municipalidad"].isin(tipos_sel))
    & (df_muni["Clasificacion_ATM"].fillna("Sin Tramo").isin(tramos_sel))
].copy()

df_mad_f = df_mad[
    (df_mad["Region_Natural"].isin(regiones_sel))
    & (df_mad["Tipo_Municipalidad"].isin(tipos_sel))
    & (df_mad["Clasificacion_ATM"].fillna("Sin Tramo").isin(tramos_sel))
].copy()

df_m_unique = df_m_f.drop_duplicates(subset=["SEC_EJEC"])
n_munis = df_m_unique["SEC_EJEC"].nunique()
rec_tot_m = df_m_f["Recaudado"].sum() / 1e6
rec_pred_m = df_m_f["Recaudado_Predial"].sum() / 1e6
pers_atm = df_m_unique["P32_1_T_Personal_Trib"].fillna(0).sum()
rec_por_emp = (df_m_f["Recaudado_Predial"].sum() / pers_atm / 1e3) if pers_atm > 0 else 0.0
tic_prom = df_m_unique["Indice_TIC_Rentas"].fillna(0).mean() if n_munis > 0 else 0.0
pct_coac = (df_m_unique["P33A"].fillna(0).mean() * 100.0) if n_munis > 0 else 0.0

# ======================== KPI CARDS ========================
c1, c2, c3, c4, c5 = st.columns(5)
kpis = [
    (c1, "Municipalidades Activas", f"{n_munis:,}"),
    (c2, "Recaudado Total SIAF", f"S/ {rec_tot_m:,.1f} M"),
    (c3, "Personal ATM (P32_1_T)", f"{int(pers_atm):,}"),
    (c4, "Predial / Empleado ATM", f"S/ {rec_por_emp:,.1f} K"),
    (c5, "Índice TIC Prom. | Coactivo", f"{tic_prom:.1f}% | {pct_coac:.1f}%"),
]
for col_ui, label, val in kpis:
    with col_ui:
        st.markdown(
            f'<div class="kpi-card"><div class="kpi-label">{label}</div><div class="kpi-val">{val}</div></div>',
            unsafe_allow_html=True,
        )

st.divider()

tab1, tab2, tab3 = st.tabs([
    "1. Capacidad Operativa RRHH & TIC (D3)",
    "2. Madurez ATM y Coactiva (D6)",
    "3. Explorador Municipal",
])

with tab1:
    g1, g2 = st.columns(2)
    df_m_unique["Perfil_Digital"] = df_m_unique.apply(
        lambda r: "Software + Catastro"
        if r.get("Software_Tributario", 0) == 1 and r.get("Catastro_Urbano", 0) == 1
        else ("Solo Software o Catastro" if (r.get("Software_Tributario", 0) == 1 or r.get("Catastro_Urbano", 0) == 1) else "Gestión Manual"),
        axis=1,
    )
    muni_tot = (
        df_m_f.groupby("SEC_EJEC", as_index=False)[["Recaudado", "Recaudado_Predial", "Ingresos_Propios"]]
        .sum()
        .merge(df_m_unique[["SEC_EJEC", "Perfil_Digital", "P32_1_T_Personal_Trib", "Indice_TIC_Rentas", "Tipo_Municipalidad"]], on="SEC_EJEC", how="inner")
    )
    df_dig = (
        muni_tot.groupby("Perfil_Digital", as_index=False)
        .agg(
            Municipalidades=("SEC_EJEC", "count"),
            Personal_ATM_Prom=("P32_1_T_Personal_Trib", "mean"),
            Predial_Prom_M=("Recaudado_Predial", lambda s: s.mean() / 1e6),
        )
    )
    with g1:
        fig_dig = px.bar(
            df_dig,
            x="Perfil_Digital",
            y="Predial_Prom_M",
            color="Perfil_Digital",
            text=df_dig["Predial_Prom_M"].map(lambda v: f"S/ {v:.2f} M"),
            title="Recaudación Predial Promedio según Madurez Digital (RENAMU P17_7 & P17_8)",
            color_discrete_sequence=[PRIMARY, ACCENT, AMBER],
        )
        fig_dig.update_layout(showlegend=False, yaxis_title="Millones de S/", template="plotly_white")
        st.plotly_chart(fig_dig, use_container_width=True)

    with g2:
        df_reg = (
            df_m_f.groupby(["Region_Natural", "Tipo_Municipalidad"], as_index=False)[["Ingresos_Propios", "Transferencias", "Recaudado"]]
            .sum()
        )
        df_reg["Autonomia_Pct"] = (df_reg["Ingresos_Propios"] / df_reg["Recaudado"].replace(0, pd.NA)) * 100
        fig_aut = px.bar(
            df_reg,
            x="Region_Natural",
            y="Autonomia_Pct",
            color="Tipo_Municipalidad",
            barmode="group",
            text=df_reg["Autonomia_Pct"].fillna(0).map(lambda v: f"{v:.1f}%"),
            title="% Autonomía Fiscal (Propios 08+09 / Total) por Región y Tipología",
            color_discrete_sequence=[PRIMARY, TEAL],
        )
        fig_aut.update_layout(yaxis_title="% Autonomía Fiscal", template="plotly_white")
        st.plotly_chart(fig_aut, use_container_width=True)

with tab2:
    if len(df_mad_f) == 0:
        st.info("No hay datos de cuestionarios ATM para los filtros seleccionados.")
    else:
        anio_camp = int(df_mad_f["ANO_APLICACION"].max())
        df_camp = df_mad_f[df_mad_f["ANO_APLICACION"] == anio_camp].copy()
        tramo_res = (
            df_camp.groupby("Clasificacion_ATM", as_index=False)
            .agg(
                Municipalidades=("SEC_EJEC", "nunique"),
                Madurez_Prom=("Puntaje_Madurez_ATM", "mean"),
                Pct_Coactivo=("Tiene_Coactivo_Activo", lambda s: s.mean() * 100),
                Pct_Sistema=("Tiene_Sistema_Rentas", lambda s: s.mean() * 100),
                Saldo_Moroso_M=("Monto_Saldo_Ord", lambda s: s.sum() / 1e6),
            )
            .sort_values("Clasificacion_ATM")
        )
        c_left, c_right = st.columns(2)
        with c_left:
            fig_mad = go.Figure()
            fig_mad.add_trace(go.Bar(
                x=tramo_res["Clasificacion_ATM"],
                y=tramo_res["Madurez_Prom"],
                name="Madurez ATM (0-100)",
                marker_color=PRIMARY,
                text=tramo_res["Madurez_Prom"].round(1),
                textposition="outside",
            ))
            fig_mad.add_trace(go.Scatter(
                x=tramo_res["Clasificacion_ATM"],
                y=tramo_res["Pct_Coactivo"],
                name="% Con Coactivo Activo",
                mode="lines+markers",
                line=dict(color=RED, width=3),
            ))
            fig_mad.update_layout(
                title=f"Madurez Institucional y Cobertura Coactiva por Tramo ATM ({anio_camp})",
                yaxis_title="Puntaje / %",
                template="plotly_white",
            )
            st.plotly_chart(fig_mad, use_container_width=True)

        with c_right:
            df_ef = (
                df_tramo[df_tramo["Region_Natural"].isin(regiones_sel) & df_tramo["Tipo_Municipalidad"].isin(tipos_sel)]
                .groupby("Clasificacion_ATM", as_index=False)[["Emision_Predial", "Recaudado_Ord", "Recaudado_Coact"]]
                .sum()
            )
            df_ef["Efectividad_Pct"] = ((df_ef["Recaudado_Ord"] + df_ef["Recaudado_Coact"]) / df_ef["Emision_Predial"].replace(0, pd.NA)) * 100
            fig_ef = px.bar(
                df_ef.sort_values("Clasificacion_ATM"),
                x="Clasificacion_ATM",
                y="Efectividad_Pct",
                text=df_ef.sort_values("Clasificacion_ATM")["Efectividad_Pct"].fillna(0).map(lambda v: f"{v:.1f}%"),
                title="Efectividad de Cobranza Predial (Ord + Coact / Emisión) por Tramo ATM",
                color_discrete_sequence=[TEAL],
            )
            fig_ef.update_layout(yaxis_title="% Efectividad", template="plotly_white")
            st.plotly_chart(fig_ef, use_container_width=True)

with tab3:
    cols_show = [
        "SEC_EJEC", "Nombre_Municipalidad", "Departamento", "Region_Natural",
        "Tipo_Municipalidad", "Clasificacion_ATM", "P32_1_T_Personal_Trib",
        "Personal_Total", "Indice_TIC_Rentas", "P33A", "PIM", "Recaudado", "Recaudado_Predial",
    ]
    tabla = (
        df_m_f.groupby(cols_show[:10], dropna=False, as_index=False)[["PIM", "Recaudado", "Recaudado_Predial"]]
        .sum()
        .sort_values("Recaudado", ascending=False)
    )
    st.dataframe(tabla.head(100), use_container_width=True)
