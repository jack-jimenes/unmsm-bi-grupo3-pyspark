"""Visor Dash by Plotly: Mapa Coroplético del Perú y Autonomía Fiscal Municipal."""

import json
from pathlib import Path
import dash
import dash_bootstrap_components as dbc
from dash import Input, Output, dcc, html
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

PRIMARY = "#0F2C59"
ACCENT = "#1E4E8C"
TEAL = "#1B7F5B"
AMBER = "#D97706"

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MARTS_DIR = PROJECT_ROOT / "gold_marts"
GEOJSON_PATH = PROJECT_ROOT / "assets" / "peru_departamentos.geojson"

df_rubro = pd.read_parquet(MARTS_DIR / "mart_depto_rubro_anual")
df_rubro = df_rubro[df_rubro["Cod_Departamento"].isin([f"{i:02d}" for i in range(1, 26)])].copy()

with open(GEOJSON_PATH, "r", encoding="utf-8") as f:
    PERU_GEOJSON = json.load(f)

ANIOS = sorted(df_rubro["Anio"].dropna().unique().tolist())
TIPOS = sorted(df_rubro["Tipo_Municipalidad"].dropna().unique().tolist())
REGIONES = sorted([r for r in df_rubro["Region_Natural"].dropna().unique() if r != "Sin Dato"])

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.FLATLY])
app.title = "MEF Municipal | Mapa Territorial y Autonomía Fiscal"

app.layout = dbc.Container(
    [
        html.Div(
            [
                html.H2("Observatorio Territorial Municipal — Mapa Coroplético y Dependencia Fiscal", style={"margin": 0}),
                html.P(
                    "Gobiernos Locales (NIVEL_GOBIERNO = 'M') | Ingresos Propios (Rubros 08 + 09) vs. Transferencias (Rubros 07 + 18)",
                    style={"margin": "4px 0 0 0", "opacity": 0.9},
                ),
            ],
            style={
                "backgroundColor": PRIMARY,
                "color": "#FFFFFF",
                "padding": "18px 24px",
                "borderRadius": "10px",
                "marginBottom": "18px",
            },
        ),
        dbc.Row(
            [
                dbc.Col(
                    [
                        dbc.Card(
                            dbc.CardBody(
                                [
                                    html.H5("Filtros Territoriales", style={"color": PRIMARY, "fontWeight": "700"}),
                                    html.Label("Rango de Años", className="fw-bold mt-2"),
                                    dcc.RangeSlider(
                                        id="filtro-anios",
                                        min=int(min(ANIOS)),
                                        max=int(max(ANIOS)),
                                        value=[max(2016, int(min(ANIOS))), int(max(ANIOS))],
                                        marks={int(a): str(a) for a in ANIOS[::3]},
                                        step=1,
                                    ),
                                    html.Label("Tipo de Municipalidad", className="fw-bold mt-4"),
                                    dcc.Dropdown(
                                        id="filtro-tipo",
                                        options=[{"label": t, "value": t} for t in TIPOS],
                                        value=TIPOS,
                                        multi=True,
                                    ),
                                    html.Label("Región Natural", className="fw-bold mt-3"),
                                    dcc.Dropdown(
                                        id="filtro-region",
                                        options=[{"label": r, "value": r} for r in REGIONES],
                                        value=REGIONES,
                                        multi=True,
                                    ),
                                    html.Label("Métrica del Mapa", className="fw-bold mt-3"),
                                    dcc.RadioItems(
                                        id="metrica-mapa",
                                        options=[
                                            {"label": " % Autonomía Fiscal (Propios 08+09)", "value": "Autonomia_Pct"},
                                            {"label": " Recaudado Total (Millones S/)", "value": "Recaudado_M"},
                                        ],
                                        value="Autonomia_Pct",
                                        className="mt-1",
                                    ),
                                ]
                            ),
                            style={"borderLeft": f"4px solid {PRIMARY}"},
                        ),
                    ],
                    width=3,
                ),
                dbc.Col(
                    [
                        dbc.Row(id="kpi-row", className="mb-3"),
                        dbc.Row(
                            [
                                dbc.Col(dcc.Graph(id="fig-mapa-peru"), width=6),
                                dbc.Col(dcc.Graph(id="fig-propios-vs-transf"), width=6),
                            ]
                        ),
                        dbc.Row(
                            [
                                dbc.Col(dcc.Graph(id="fig-tendencia-autonomia"), width=12),
                            ]
                        ),
                    ],
                    width=9,
                ),
            ]
        ),
    ],
    fluid=True,
    style={"backgroundColor": "#F8FAFC", "padding": "20px", "minHeight": "100vh"},
)


@app.callback(
    [
        Output("kpi-row", "children"),
        Output("fig-mapa-peru", "figure"),
        Output("fig-propios-vs-transf", "figure"),
        Output("fig-tendencia-autonomia", "figure"),
    ],
    [
        Input("filtro-anios", "value"),
        Input("filtro-tipo", "value"),
        Input("filtro-region", "value"),
        Input("metrica-mapa", "value"),
    ],
)
def actualizar_tablero(rango_anios, tipos_sel, regiones_sel, metrica):
    tipos_sel = tipos_sel or TIPOS
    regiones_sel = regiones_sel or REGIONES

    dff = df_rubro[
        (df_rubro["Anio"].between(rango_anios[0], rango_anios[1]))
        & (df_rubro["Tipo_Municipalidad"].isin(tipos_sel))
        & (df_rubro["Region_Natural"].isin(regiones_sel))
    ].copy()

    dff["Ingresos_Propios"] = dff["Recaudado"].where(dff["RUBRO"].isin(["08", "09"]), 0.0)
    dff["Transferencias"] = dff["Recaudado"].where(dff["RUBRO"].isin(["07", "18"]), 0.0)

    rec_tot = dff["Recaudado"].sum()
    pim_tot = dff["PIM"].sum()
    prop_tot = dff["Ingresos_Propios"].sum()
    transf_tot = dff["Transferencias"].sum()
    aut_pct = (prop_tot / rec_tot * 100.0) if rec_tot else 0.0
    ejec_pct = (rec_tot / pim_tot * 100.0) if pim_tot else 0.0

    def card(titulo, valor, color=PRIMARY):
        return dbc.Col(
            dbc.Card(
                dbc.CardBody(
                    [
                        html.Small(titulo, className="text-muted fw-bold"),
                        html.H4(valor, style={"color": color, "fontWeight": "700", "margin": "4px 0 0 0"}),
                    ]
                ),
                style={"borderTop": f"4px solid {color}"},
            ),
            width=3,
        )

    kpis = [
        card("Recaudado Municipal", f"S/ {rec_tot/1e6:,.1f} M", PRIMARY),
        card("Ejecución vs PIM", f"{ejec_pct:.1f}%", ACCENT),
        card("% Autonomía Fiscal (08+09)", f"{aut_pct:.1f}%", TEAL),
        card("Dependencia Transf. (07+18)", f"{(transf_tot/rec_tot*100.0 if rec_tot else 0.0):.1f}%", AMBER),
    ]

    dep = (
        dff.groupby(["Cod_Departamento", "Departamento", "Region_Natural"], as_index=False)[
            ["Recaudado", "Ingresos_Propios", "Transferencias"]
        ]
        .sum()
    )
    dep["Recaudado_M"] = dep["Recaudado"] / 1e6
    dep["Autonomia_Pct"] = (dep["Ingresos_Propios"] / dep["Recaudado"].replace(0, pd.NA) * 100.0).fillna(0.0)

    fig_map = px.choropleth(
        dep,
        geojson=PERU_GEOJSON,
        locations="Cod_Departamento",
        featureidkey="properties.FIRST_IDDP",
        color=metrica,
        hover_name="Departamento",
        hover_data={"Cod_Departamento": True, "Autonomia_Pct": ":.1f", "Recaudado_M": ":,.1f"},
        color_continuous_scale="Blues",
        title="Mapa Coroplético del Perú por Departamento",
    )
    fig_map.update_geos(fitbounds="locations", visible=False)
    fig_map.update_layout(margin={"r": 0, "t": 45, "l": 0, "b": 0}, height=460)

    top_dep = dep.sort_values("Autonomia_Pct", ascending=True).tail(12)
    fig_bar = go.Figure()
    fig_bar.add_trace(go.Bar(
        y=top_dep["Departamento"], x=top_dep["Ingresos_Propios"] / 1e6,
        name="Propios (08+09)", orientation="h", marker_color=PRIMARY,
    ))
    fig_bar.add_trace(go.Bar(
        y=top_dep["Departamento"], x=top_dep["Transferencias"] / 1e6,
        name="Transferencias (07+18)", orientation="h", marker_color=AMBER,
    ))
    fig_bar.update_layout(
        barmode="stack",
        title="Top 12 Departamentos por Autonomía: Ingresos Propios vs. Transferencias (Millones S/)",
        xaxis_title="Millones de S/",
        template="plotly_white",
        height=460,
    )

    serie = dff.groupby("Anio", as_index=False)[["Recaudado", "Ingresos_Propios", "Transferencias"]].sum().sort_values("Anio")
    serie["Autonomia_Pct"] = (serie["Ingresos_Propios"] / serie["Recaudado"].replace(0, pd.NA) * 100.0).fillna(0.0)
    fig_ts = px.line(
        serie,
        x="Anio",
        y="Autonomia_Pct",
        markers=True,
        title="Evolución Histórica de la Autonomía Fiscal Municipal (%)",
        color_discrete_sequence=[PRIMARY],
    )
    fig_ts.update_layout(yaxis_title="% Ingresos Propios (08+09)", template="plotly_white", height=320)

    return kpis, fig_map, fig_bar, fig_ts


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)
