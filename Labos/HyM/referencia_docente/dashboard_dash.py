"""
Dashboard Prescriptivo con Dash + PySpark
pip install dash dash-bootstrap-components plotly
pip install typing_extensions==4.12.2

python dashboard_dash.py

http://127.0.0.1:3050/
"""

import dash
from dash import dcc, html, Input, Output
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month
import numpy as np

# ============================================================================
# CONFIGURACIÓN DE SPARK
# ============================================================================

def get_spark_session():
    """Inicializa sesión de Spark optimizada"""
    spark = SparkSession.builder \
        .appName("DashboardPrescriptivo") \
        .config("spark.ui.port", "4030") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "10") \
        .getOrCreate()
    return spark

def load_data():
    """Carga y procesa datos desde Parquet"""
    spark = get_spark_session()
    
    try:
        df_spark = spark.read.option("mergeSchema", "true") \
            .parquet("./gold/consolidado_prescriptivo.parquet")
        
        print("Datos cargados exitosamente desde Parquet")
        print(f"Total de registros: {df_spark.count():,}")
        
        # Agregar columnas derivadas
        if 'ultima_compra' in df_spark.columns:
            df_spark = df_spark.withColumn("año", year(col("ultima_compra"))) \
                               .withColumn("mes", month(col("ultima_compra")))
        
        # Convertir a Pandas
        df_pandas = df_spark.toPandas()
        
        # Limpiar y validar datos
        if 'age' in df_pandas.columns:
            df_pandas['age'] = pd.to_numeric(df_pandas['age'], errors='coerce').fillna(30).astype(int)
        
        if 'gasto_total' in df_pandas.columns:
            df_pandas['gasto_total'] = pd.to_numeric(df_pandas['gasto_total'], errors='coerce').fillna(0)
        
        if 'probabilidad_compra' in df_pandas.columns:
            df_pandas['probabilidad_compra'] = pd.to_numeric(df_pandas['probabilidad_compra'], errors='coerce').fillna(0)
        
        if 'total_compras' in df_pandas.columns:
            df_pandas['total_compras'] = pd.to_numeric(df_pandas['total_compras'], errors='coerce').fillna(0)
        
        # Asegurar que existan columnas clave
        for col_name in ['segmento_nombre', 'estrategia', 'canal_recomendado', 'frecuencia_contacto', 'nivel_descuento']:
            if col_name not in df_pandas.columns:
                df_pandas[col_name] = 'No disponible'
        
        # Valores por defecto para año y mes si no existen
        if 'año' not in df_pandas.columns:
            df_pandas['año'] = 2024
        if 'mes' not in df_pandas.columns:
            df_pandas['mes'] = 1
        
        print(f"Datos procesados: {len(df_pandas):,} registros")
        print(f"Columnas: {list(df_pandas.columns)}")
        
        return df_pandas
        
    except Exception as e:
        print(f"Error cargando datos: {e}")
        raise

# Cargar datos
print("Iniciando carga de datos...")
df = load_data()

# ============================================================================
# INICIALIZACIÓN DE DASH
# ============================================================================

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.CYBORG],
    suppress_callback_exceptions=True
)

# ============================================================================
# ESTILOS
# ============================================================================

CARD_STYLE = {
    'backgroundColor': '#1e1e1e',
    'border': '1px solid #333',
    'borderRadius': '10px',
    'padding': '20px',
    'boxShadow': '0 4px 6px rgba(0, 0, 0, 0.3)',
    'marginBottom': '20px'
}

HEADER_STYLE = {
    'textAlign': 'center',
    'color': '#00d9ff',
    'fontSize': '48px',
    'fontWeight': 'bold',
    'marginBottom': '30px',
    'textShadow': '0 0 10px rgba(0, 217, 255, 0.5)'
}

METRIC_CARD_STYLE = {
    'backgroundColor': '#2a2a2a',
    'borderRadius': '10px',
    'padding': '15px',
    'textAlign': 'center',
    'border': '2px solid #00d9ff',
    'boxShadow': '0 0 20px rgba(0, 217, 255, 0.3)',
    'minHeight': '100px'
}

# ============================================================================
# LAYOUT
# ============================================================================

app.layout = dbc.Container([
    
    html.Div([
        html.H1("Dashboard Prescriptivo Inteligente", style=HEADER_STYLE),
        html.P(
            f"Analizando {len(df):,} clientes | PySpark + Dash",
            style={'textAlign': 'center', 'color': '#888', 'fontSize': '20px'}
        )
    ]),
    
    html.Hr(style={'borderColor': '#333'}),
    
    dbc.Row([
        # SIDEBAR - FILTROS
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("FILTROS", style={'fontWeight': 'bold', 'fontSize': '18px', 'color': '#00d9ff'}),
                dbc.CardBody([
                    
                    html.Label("Año", style={'fontWeight': 'bold', 'color': '#fff'}),
                    dcc.Dropdown(
                        id='filtro-año',
                        options=[{'label': str(año), 'value': año} for año in sorted(df['año'].unique())],
                        value=sorted(df['año'].unique()),
                        multi=True,
                        style={'marginBottom': '20px'}
                    ),
                    
                    html.Label("Edad", style={'fontWeight': 'bold', 'color': '#fff'}),
                    dcc.RangeSlider(
                        id='filtro-edad',
                        min=int(df['age'].min()),
                        max=int(df['age'].max()),
                        value=[int(df['age'].min()), int(df['age'].max())],
                        marks={i: str(i) for i in range(int(df['age'].min()), int(df['age'].max())+1, 10)},
                        tooltip={"placement": "bottom", "always_visible": True}
                    ),
                    
                    html.Label("Segmento", style={'fontWeight': 'bold', 'color': '#fff', 'marginTop': '30px'}),
                    dcc.Dropdown(
                        id='filtro-segmento',
                        options=[{'label': seg, 'value': seg} for seg in sorted(df['segmento_nombre'].unique())],
                        value=sorted(df['segmento_nombre'].unique()),
                        multi=True,
                        style={'marginBottom': '20px'}
                    ),
                    
                    html.Label("Estrategia", style={'fontWeight': 'bold', 'color': '#fff'}),
                    dcc.Dropdown(
                        id='filtro-estrategia',
                        options=[{'label': est, 'value': est} for est in sorted(df['estrategia'].unique())],
                        value=sorted(df['estrategia'].unique()),
                        multi=True
                    ),
                    
                ])
            ], style=CARD_STYLE)
        ], width=3),
        
        # CONTENIDO PRINCIPAL
        dbc.Col([
            
            # MÉTRICAS
            dbc.Row([
                dbc.Col(html.Div(id='metric-clientes', style=METRIC_CARD_STYLE), width=2),
                dbc.Col(html.Div(id='metric-gasto-total', style=METRIC_CARD_STYLE), width=2),
                dbc.Col(html.Div(id='metric-gasto-promedio', style=METRIC_CARD_STYLE), width=2),
                dbc.Col(html.Div(id='metric-propension', style=METRIC_CARD_STYLE), width=2),
                dbc.Col(html.Div(id='metric-edad', style=METRIC_CARD_STYLE), width=2),
                dbc.Col(html.Div(id='metric-compras', style=METRIC_CARD_STYLE), width=2),
            ], style={'marginBottom': '30px'}),
            
            # GRÁFICOS FILA 1
            dbc.Row([
                dbc.Col(dcc.Graph(id='graph-segmento'), width=6),
                dbc.Col(dcc.Graph(id='graph-estrategia'), width=6),
            ]),
            
            # GRÁFICOS FILA 2
            dbc.Row([
                dbc.Col(dcc.Graph(id='graph-gasto-segmento'), width=6),
                dbc.Col(dcc.Graph(id='graph-probabilidad'), width=6),
            ]),
            
            # GRÁFICOS FILA 3
            dbc.Row([
                dbc.Col(dcc.Graph(id='graph-edad'), width=6),
                dbc.Col(dcc.Graph(id='graph-scatter'), width=6),
            ]),
            
            # GRÁFICOS FILA 4
            dbc.Row([
                dbc.Col(dcc.Graph(id='graph-canal'), width=4),
                dbc.Col(dcc.Graph(id='graph-frecuencia'), width=4),
                dbc.Col(dcc.Graph(id='graph-descuento'), width=4),
            ]),
            
            # GRÁFICOS FILA 5
            dbc.Row([
                dbc.Col(dcc.Graph(id='graph-temporal'), width=12),
            ]),
            
            # GRÁFICOS FILA 6
            dbc.Row([
                dbc.Col(dcc.Graph(id='graph-heatmap'), width=6),
                dbc.Col(dcc.Graph(id='graph-sunburst'), width=6),
            ]),
            
        ], width=9),
    ]),
    
], fluid=True, style={'backgroundColor': '#0a0a0a', 'minHeight': '100vh', 'padding': '30px'})

# ============================================================================
# CALLBACKS
# ============================================================================

@app.callback(
    [
        Output('metric-clientes', 'children'),
        Output('metric-gasto-total', 'children'),
        Output('metric-gasto-promedio', 'children'),
        Output('metric-propension', 'children'),
        Output('metric-edad', 'children'),
        Output('metric-compras', 'children'),
        Output('graph-segmento', 'figure'),
        Output('graph-estrategia', 'figure'),
        Output('graph-gasto-segmento', 'figure'),
        Output('graph-probabilidad', 'figure'),
        Output('graph-edad', 'figure'),
        Output('graph-scatter', 'figure'),
        Output('graph-canal', 'figure'),
        Output('graph-frecuencia', 'figure'),
        Output('graph-descuento', 'figure'),
        Output('graph-temporal', 'figure'),
        Output('graph-heatmap', 'figure'),
        Output('graph-sunburst', 'figure'),
    ],
    [
        Input('filtro-año', 'value'),
        Input('filtro-edad', 'value'),
        Input('filtro-segmento', 'value'),
        Input('filtro-estrategia', 'value'),
    ]
)
def update_dashboard(años, rango_edad, segmentos, estrategias):
    
    # Filtrar datos
    df_filtered = df[
        (df['año'].isin(años if años else df['año'].unique())) &
        (df['age'] >= rango_edad[0]) &
        (df['age'] <= rango_edad[1]) &
        (df['segmento_nombre'].isin(segmentos if segmentos else df['segmento_nombre'].unique())) &
        (df['estrategia'].isin(estrategias if estrategias else df['estrategia'].unique()))
    ]
    
    # ========== MÉTRICAS ==========
    metric_clientes = [
        html.H3(f"{len(df_filtered):,}", style={'color': '#00d9ff', 'fontSize': '32px'}),
        html.P("Clientes", style={'color': '#888', 'fontSize': '12px'})
    ]
    
    metric_gasto_total = [
        html.H3(f"${df_filtered['gasto_total'].sum():,.0f}", style={'color': '#00ff88', 'fontSize': '28px'}),
        html.P("Gasto Total", style={'color': '#888', 'fontSize': '12px'})
    ]
    
    metric_gasto_promedio = [
        html.H3(f"${df_filtered['gasto_total'].mean():,.0f}", style={'color': '#ffa500', 'fontSize': '28px'}),
        html.P("Gasto Promedio", style={'color': '#888', 'fontSize': '12px'})
    ]
    
    metric_propension = [
        html.H3(f"{df_filtered['probabilidad_compra'].mean():.1%}", style={'color': '#ff4500', 'fontSize': '32px'}),
        html.P("Propensión", style={'color': '#888', 'fontSize': '12px'})
    ]
    
    metric_edad = [
        html.H3(f"{df_filtered['age'].mean():.0f}", style={'color': '#da70d6', 'fontSize': '32px'}),
        html.P("Edad Media", style={'color': '#888', 'fontSize': '12px'})
    ]
    
    metric_compras = [
        html.H3(f"{df_filtered['total_compras'].sum():,.0f}", style={'color': '#ffd700', 'fontSize': '28px'}),
        html.P("Total Compras", style={'color': '#888', 'fontSize': '12px'})
    ]
    
    # ========== GRÁFICOS ==========
    
    # 1. Segmento
    segmento_counts = df_filtered['segmento_nombre'].value_counts()
    fig_segmento = go.Figure(data=[go.Pie(
        labels=segmento_counts.index,
        values=segmento_counts.values,
        hole=0.5,
        marker=dict(colors=px.colors.sequential.Plasma_r),
        textinfo='percent+label'
    )])
    fig_segmento.update_layout(
        title="Distribución por Segmento",
        template='plotly_dark',
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        height=400
    )
    
    # 2. Estrategia
    estrategia_counts = df_filtered['estrategia'].value_counts()
    fig_estrategia = go.Figure(data=[go.Bar(
        x=estrategia_counts.index,
        y=estrategia_counts.values,
        marker=dict(
            color=estrategia_counts.values,
            colorscale='Turbo',
            showscale=True
        ),
        text=estrategia_counts.values,
        textposition='outside'
    )])
    fig_estrategia.update_layout(
        title="Estrategias Recomendadas",
        template='plotly_dark',
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        height=400
    )
    
    # 3. Gasto por Segmento
    gasto_segmento = df_filtered.groupby('segmento_nombre')['gasto_total'].sum().sort_values()
    fig_gasto = go.Figure(go.Bar(
        x=gasto_segmento.values,
        y=gasto_segmento.index,
        orientation='h',
        marker=dict(
            color=gasto_segmento.values,
            colorscale='Viridis',
            showscale=True
        ),
        text=[f'${x:,.0f}' for x in gasto_segmento.values],
        textposition='auto'
    ))
    fig_gasto.update_layout(
        title="Gasto Total por Segmento",
        template='plotly_dark',
        paper_bgcolor='rgba(0,0,0,0)',
        height=400
    )
    
    # 4. Probabilidad (Box Plot)
    fig_prob = px.box(
        df_filtered,
        x='estrategia',
        y='probabilidad_compra',
        color='estrategia',
        title='Propensión por Estrategia',
        color_discrete_sequence=px.colors.qualitative.Vivid
    )
    fig_prob.update_layout(
        template='plotly_dark',
        paper_bgcolor='rgba(0,0,0,0)',
        showlegend=False,
        height=400
    )
    
    # 5. Edad
    fig_edad = px.histogram(
        df_filtered,
        x='age',
        nbins=30,
        title='Distribución de Edad',
        color_discrete_sequence=['#00d9ff'],
        marginal='box'
    )
    fig_edad.update_layout(
        template='plotly_dark',
        paper_bgcolor='rgba(0,0,0,0)',
        height=400
    )
    
    # 6. Scatter
    sample = df_filtered.sample(min(1000, len(df_filtered))) if len(df_filtered) > 0 else df_filtered
    fig_scatter = px.scatter(
        sample,
        x='gasto_total',
        y='probabilidad_compra',
        size='total_compras',
        color='estrategia',
        title='Gasto vs Propensión (Tamaño = Compras)',
        color_discrete_sequence=px.colors.qualitative.Bold,
        hover_data=['segmento_nombre', 'age']
    )
    fig_scatter.update_layout(
        template='plotly_dark',
        paper_bgcolor='rgba(0,0,0,0)',
        height=400
    )
    
    # 7. Canal
    canal_counts = df_filtered['canal_recomendado'].value_counts()
    fig_canal = go.Figure(data=[go.Pie(
        labels=canal_counts.index,
        values=canal_counts.values,
        hole=0.4,
        marker=dict(colors=['#ff6692', '#b6e880']),
        pull=[0.1, 0]
    )])
    fig_canal.update_layout(
        title="Canal Recomendado",
        template='plotly_dark',
        paper_bgcolor='rgba(0,0,0,0)',
        height=400
    )
    
    # 8. Frecuencia
    freq_counts = df_filtered['frecuencia_contacto'].value_counts()
    fig_freq = go.Figure(go.Bar(
        x=freq_counts.index,
        y=freq_counts.values,
        marker=dict(color=['#00cc96', '#ffa15a', '#ab63fa']),
        text=freq_counts.values,
        textposition='outside'
    ))
    fig_freq.update_layout(
        title="Frecuencia de Contacto",
        template='plotly_dark',
        paper_bgcolor='rgba(0,0,0,0)',
        height=400
    )
    
    # 9. Descuento
    desc_counts = df_filtered['nivel_descuento'].value_counts()
    fig_desc = go.Figure(go.Funnel(
        y=desc_counts.index,
        x=desc_counts.values,
        marker=dict(color=px.colors.sequential.Reds_r)
    ))
    fig_desc.update_layout(
        title="Niveles de Descuento",
        template='plotly_dark',
        paper_bgcolor='rgba(0,0,0,0)',
        height=400
    )
    
    # 10. Temporal
    compras_mes = df_filtered.groupby('mes').size().reset_index(name='Cantidad')
    meses = {1:'Ene', 2:'Feb', 3:'Mar', 4:'Abr', 5:'May', 6:'Jun', 
             7:'Jul', 8:'Ago', 9:'Sep', 10:'Oct', 11:'Nov', 12:'Dic'}
    compras_mes['Mes'] = compras_mes['mes'].map(meses)
    
    fig_temporal = go.Figure(go.Scatter(
        x=compras_mes['Mes'],
        y=compras_mes['Cantidad'],
        mode='lines+markers',
        fill='tozeroy',
        line=dict(color='#ab63fa', width=3),
        marker=dict(size=10)
    ))
    fig_temporal.update_layout(
        title="Compras por Mes",
        template='plotly_dark',
        paper_bgcolor='rgba(0,0,0,0)',
        height=400
    )
    
    # 11. Heatmap
    pivot = pd.crosstab(df_filtered['segmento_nombre'], df_filtered['estrategia'])
    fig_heatmap = go.Figure(data=go.Heatmap(
        z=pivot.values,
        x=pivot.columns,
        y=pivot.index,
        colorscale='Cividis',
        text=pivot.values,
        texttemplate='%{text}',
        textfont={"size": 14}
    ))
    fig_heatmap.update_layout(
        title="Matriz Segmento vs Estrategia",
        template='plotly_dark',
        paper_bgcolor='rgba(0,0,0,0)',
        height=500
    )
    
    # 12. Sunburst
    fig_sunburst = px.sunburst(
        df_filtered.sample(min(500, len(df_filtered))),
        path=['estrategia', 'segmento_nombre', 'canal_recomendado'],
        title='Jerarquía: Estrategia -> Segmento -> Canal',
        color='estrategia',
        color_discrete_sequence=px.colors.qualitative.Pastel
    )
    fig_sunburst.update_layout(
        template='plotly_dark',
        paper_bgcolor='rgba(0,0,0,0)',
        height=500
    )
    
    return (
        metric_clientes, metric_gasto_total, metric_gasto_promedio, 
        metric_propension, metric_edad, metric_compras,
        fig_segmento, fig_estrategia, fig_gasto, fig_prob, fig_edad,
        fig_scatter, fig_canal, fig_freq, fig_desc, fig_temporal,
        fig_heatmap, fig_sunburst
    )

# ============================================================================
# EJECUTAR
# ============================================================================

if __name__ == '__main__':
    print("Dashboard iniciado correctamente")
    print(f"Abre: http://127.0.0.1:3050")
    app.run(debug=True, port=3050, host='0.0.0.0')