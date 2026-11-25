"""
Dashboard Prescriptivo con Streamlit + PySpark
pip install streamlit plotly

streamlit run dashboard_streamlit.py
streamlit run dashboard_streamlit.py --server.port 3050
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month
import numpy as np

# ============================================================================
# CONFIGURACIÓN DE PÁGINA
# ============================================================================

st.set_page_config(
    page_title="Dashboard Prescriptivo",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# ESTILOS CSS
# ============================================================================

st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        color: #00d9ff;
        text-align: center;
        text-shadow: 0 0 10px rgba(0, 217, 255, 0.5);
        margin-bottom: 1rem;
    }
    .sub-header {
        text-align: center;
        color: #888;
        font-size: 1.2rem;
        margin-bottom: 2rem;
    }
    .metric-container {
        background: linear-gradient(135deg, #1e1e1e 0%, #2a2a2a 100%);
        padding: 1.5rem;
        border-radius: 10px;
        border: 2px solid #00d9ff;
        box-shadow: 0 0 20px rgba(0, 217, 255, 0.3);
        text-align: center;
    }
    .metric-value {
        font-size: 2rem;
        font-weight: bold;
        color: #00d9ff;
    }
    .metric-label {
        font-size: 0.9rem;
        color: #888;
        margin-top: 0.5rem;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 2rem;
    }
    .stTabs [data-baseweb="tab"] {
        background-color: #1e1e1e;
        border-radius: 10px;
        padding: 1rem 2rem;
        color: #00d9ff;
    }
    .stTabs [aria-selected="true"] {
        background-color: #00d9ff;
        color: #000;
    }
    div[data-testid="stSidebarContent"] {
        background: linear-gradient(180deg, #0a0a0a 0%, #1e1e1e 100%);
    }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# FUNCIONES DE CARGA DE DATOS
# ============================================================================

@st.cache_resource
def get_spark_session():
    """Inicializa sesión de Spark"""
    spark = SparkSession.builder \
        .appName("StreamlitDashboard") \
        .config("spark.ui.port", "4020") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "10") \
        .getOrCreate()
    return spark

@st.cache_data
def load_data():
    """Carga y procesa datos desde Parquet"""
    spark = get_spark_session()
    
    try:
        df_spark = spark.read.option("mergeSchema", "true") \
            .parquet("./gold/consolidado_prescriptivo.parquet")
        
        # Agregar columnas derivadas
        if 'ultima_compra' in df_spark.columns:
            df_spark = df_spark.withColumn("año", year(col("ultima_compra"))) \
                               .withColumn("mes", month(col("ultima_compra")))
        
        # Convertir a Pandas
        df_pandas = df_spark.toPandas()
        
        # Limpiar datos numéricos
        numeric_cols = ['age', 'gasto_total', 'probabilidad_compra', 'total_compras']
        for col_name in numeric_cols:
            if col_name in df_pandas.columns:
                df_pandas[col_name] = pd.to_numeric(df_pandas[col_name], errors='coerce')
                df_pandas[col_name] = df_pandas[col_name].fillna(df_pandas[col_name].median())
        
        # Asegurar columnas clave
        required_cols = ['segmento_nombre', 'estrategia', 'canal_recomendado', 
                        'frecuencia_contacto', 'nivel_descuento', 'customer_id']
        for col_name in required_cols:
            if col_name not in df_pandas.columns:
                df_pandas[col_name] = 'No disponible'
        
        # Valores por defecto para año y mes
        if 'año' not in df_pandas.columns:
            df_pandas['año'] = 2024
        if 'mes' not in df_pandas.columns:
            df_pandas['mes'] = 1
        
        return df_pandas
        
    except Exception as e:
        st.error(f"Error cargando datos: {e}")
        st.stop()

# ============================================================================
# CARGAR DATOS
# ============================================================================

with st.spinner("Cargando datos..."):
    df = load_data()

# ============================================================================
# HEADER
# ============================================================================

st.markdown('<p class="main-header">Dashboard Prescriptivo Inteligente</p>', unsafe_allow_html=True)
st.markdown(f'<p class="sub-header">Analizando {len(df):,} clientes | PySpark + Streamlit</p>', unsafe_allow_html=True)

st.divider()

# ============================================================================
# SIDEBAR - FILTROS
# ============================================================================

st.sidebar.header("FILTROS AVANZADOS")

# Filtro de Cliente Individual (NUEVO)
st.sidebar.subheader("Búsqueda Individual")
buscar_cliente = st.sidebar.checkbox("Buscar por Cliente Específico")

cliente_seleccionado = None
if buscar_cliente:
    # Buscar por ID completo o parcial
    customer_search = st.sidebar.text_input(
        "ID del Cliente (busca parcial)",
        placeholder="Escribe parte del ID...",
        help="Puedes buscar con parte del customer_id"
    )
    
    if customer_search:
        clientes_filtrados = df[df['customer_id'].str.contains(customer_search, case=False, na=False)]
        
        if len(clientes_filtrados) > 0:
            opciones_clientes = clientes_filtrados['customer_id'].tolist()
            cliente_seleccionado = st.sidebar.selectbox(
                "Selecciona el cliente",
                options=opciones_clientes,
                help=f"Encontrados {len(opciones_clientes)} clientes"
            )
            
            # Mostrar info del cliente seleccionado
            if cliente_seleccionado:
                cliente_info = df[df['customer_id'] == cliente_seleccionado].iloc[0]
                st.sidebar.success("Cliente encontrado")
                st.sidebar.info(f"""
                **Segmento:** {cliente_info['segmento_nombre']}  
                **Estrategia:** {cliente_info['estrategia']}  
                **Gasto Total:** ${cliente_info['gasto_total']:,.0f}  
                **Propensión:** {cliente_info['probabilidad_compra']:.1%}
                """)
        else:
            st.sidebar.warning("No se encontraron clientes con ese ID")

st.sidebar.divider()

# Filtros generales (solo si NO hay cliente específico seleccionado)
if not cliente_seleccionado:
    st.sidebar.subheader("Filtros Generales")
    
    # Filtro Año
    años_disponibles = sorted(df['año'].unique())
    años_seleccionados = st.sidebar.multiselect(
        "Año",
        options=años_disponibles,
        default=años_disponibles
    )
    
    # Filtro Edad
    edad_min = int(df['age'].min())
    edad_max = int(df['age'].max())
    rango_edad = st.sidebar.slider(
        "Rango de Edad",
        min_value=edad_min,
        max_value=edad_max,
        value=(edad_min, edad_max)
    )
    
    # Filtro Segmento
    segmentos_disponibles = sorted(df['segmento_nombre'].unique())
    segmentos_seleccionados = st.sidebar.multiselect(
        "Segmento",
        options=segmentos_disponibles,
        default=segmentos_disponibles
    )
    
    # Filtro Estrategia
    estrategias_disponibles = sorted(df['estrategia'].unique())
    estrategias_seleccionadas = st.sidebar.multiselect(
        "Estrategia",
        options=estrategias_disponibles,
        default=estrategias_disponibles
    )
else:
    # Si hay cliente seleccionado, usar solo ese cliente
    años_seleccionados = df['año'].unique()
    rango_edad = (int(df['age'].min()), int(df['age'].max()))
    segmentos_seleccionados = df['segmento_nombre'].unique()
    estrategias_seleccionadas = df['estrategia'].unique()

st.sidebar.divider()
st.sidebar.info(f"Dataset completo: {len(df):,} registros")

# ============================================================================
# APLICAR FILTROS
# ============================================================================

if cliente_seleccionado:
    # Filtrar por cliente específico
    df_filtered = df[df['customer_id'] == cliente_seleccionado]
    st.info(f"Mostrando datos del cliente: `{cliente_seleccionado[:16]}...`")
else:
    # Filtros normales
    df_filtered = df[
        (df['año'].isin(años_seleccionados)) &
        (df['age'] >= rango_edad[0]) &
        (df['age'] <= rango_edad[1]) &
        (df['segmento_nombre'].isin(segmentos_seleccionados)) &
        (df['estrategia'].isin(estrategias_seleccionadas))
    ]

# ============================================================================
# MÉTRICAS PRINCIPALES
# ============================================================================

col1, col2, col3, col4, col5, col6 = st.columns(6)

with col1:
    st.markdown(f"""
    <div class="metric-container">
        <div class="metric-value">{len(df_filtered):,}</div>
        <div class="metric-label">Clientes</div>
    </div>
    """, unsafe_allow_html=True)

with col2:
    gasto_total = df_filtered['gasto_total'].sum()
    st.markdown(f"""
    <div class="metric-container">
        <div class="metric-value" style="color: #00ff88;">${gasto_total:,.0f}</div>
        <div class="metric-label">Gasto Total</div>
    </div>
    """, unsafe_allow_html=True)

with col3:
    gasto_promedio = df_filtered['gasto_total'].mean()
    st.markdown(f"""
    <div class="metric-container">
        <div class="metric-value" style="color: #ffa500;">${gasto_promedio:,.0f}</div>
        <div class="metric-label">Gasto Promedio</div>
    </div>
    """, unsafe_allow_html=True)

with col4:
    propension = df_filtered['probabilidad_compra'].mean()
    st.markdown(f"""
    <div class="metric-container">
        <div class="metric-value" style="color: #ff4500;">{propension:.1%}</div>
        <div class="metric-label">Propensión</div>
    </div>
    """, unsafe_allow_html=True)

with col5:
    edad_promedio = df_filtered['age'].mean()
    st.markdown(f"""
    <div class="metric-container">
        <div class="metric-value" style="color: #da70d6;">{edad_promedio:.0f}</div>
        <div class="metric-label">Edad Media</div>
    </div>
    """, unsafe_allow_html=True)

with col6:
    total_compras = df_filtered['total_compras'].sum()
    st.markdown(f"""
    <div class="metric-container">
        <div class="metric-value" style="color: #ffd700;">{total_compras:,.0f}</div>
        <div class="metric-label">Total Compras</div>
    </div>
    """, unsafe_allow_html=True)

st.divider()

# ============================================================================
# TABS PARA ORGANIZAR GRÁFICOS
# ============================================================================

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Segmentación",
    "Análisis Financiero",
    "Demografía",
    "Recomendaciones",
    "Análisis Temporal"
])

# ============================================================================
# TAB 1: SEGMENTACIÓN
# ============================================================================

with tab1:
    col1, col2 = st.columns(2)
    
    with col1:
        # Distribución por Segmento
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
            height=450
        )
        st.plotly_chart(fig_segmento, use_container_width=True)
    
    with col2:
        # Estrategias Recomendadas
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
            height=450
        )
        st.plotly_chart(fig_estrategia, use_container_width=True)
    
    # Heatmap
    st.subheader("Matriz: Segmento vs Estrategia")
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
        template='plotly_dark',
        height=450
    )
    st.plotly_chart(fig_heatmap, use_container_width=True)

# ============================================================================
# TAB 2: ANÁLISIS FINANCIERO
# ============================================================================

with tab2:
    col1, col2 = st.columns(2)
    
    with col1:
        # Gasto por Segmento
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
            height=450
        )
        st.plotly_chart(fig_gasto, use_container_width=True)
    
    with col2:
        # Probabilidad de Compra
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
            showlegend=False,
            height=450
        )
        st.plotly_chart(fig_prob, use_container_width=True)
    
    # Scatter: Gasto vs Propensión
    st.subheader("Análisis: Gasto vs Propensión a Compra")
    sample = df_filtered.sample(min(1000, len(df_filtered))) if len(df_filtered) > 0 else df_filtered
    fig_scatter = px.scatter(
        sample,
        x='gasto_total',
        y='probabilidad_compra',
        size='total_compras',
        color='estrategia',
        title='Gasto Total vs Probabilidad de Compra (Tamaño = Total Compras)',
        color_discrete_sequence=px.colors.qualitative.Bold,
        hover_data=['segmento_nombre', 'age', 'customer_id']
    )
    fig_scatter.update_layout(
        template='plotly_dark',
        height=500
    )
    st.plotly_chart(fig_scatter, use_container_width=True)

# ============================================================================
# TAB 3: DEMOGRAFÍA
# ============================================================================

with tab3:
    col1, col2 = st.columns(2)
    
    with col1:
        # Distribución de Edad
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
            height=450
        )
        st.plotly_chart(fig_edad, use_container_width=True)
    
    with col2:
        # Edad por Segmento
        fig_edad_seg = px.box(
            df_filtered,
            x='segmento_nombre',
            y='age',
            color='segmento_nombre',
            title='Edad por Segmento',
            color_discrete_sequence=px.colors.qualitative.Pastel
        )
        fig_edad_seg.update_layout(
            template='plotly_dark',
            showlegend=False,
            height=450
        )
        st.plotly_chart(fig_edad_seg, use_container_width=True)

# ============================================================================
# TAB 4: RECOMENDACIONES
# ============================================================================

with tab4:
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Canal Recomendado
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
            height=400
        )
        st.plotly_chart(fig_canal, use_container_width=True)
    
    with col2:
        # Frecuencia de Contacto
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
            height=400
        )
        st.plotly_chart(fig_freq, use_container_width=True)
    
    with col3:
        # Niveles de Descuento
        desc_counts = df_filtered['nivel_descuento'].value_counts()
        fig_desc = go.Figure(go.Funnel(
            y=desc_counts.index,
            x=desc_counts.values,
            marker=dict(color=px.colors.sequential.Reds_r)
        ))
        fig_desc.update_layout(
            title="Niveles de Descuento",
            template='plotly_dark',
            height=400
        )
        st.plotly_chart(fig_desc, use_container_width=True)
    
    # Sunburst Jerárquico
    st.subheader("Jerarquía de Recomendaciones")
    fig_sunburst = px.sunburst(
        df_filtered.sample(min(500, len(df_filtered))),
        path=['estrategia', 'segmento_nombre', 'canal_recomendado'],
        title='Estrategia -> Segmento -> Canal',
        color='estrategia',
        color_discrete_sequence=px.colors.qualitative.Pastel
    )
    fig_sunburst.update_layout(
        template='plotly_dark',
        height=500
    )
    st.plotly_chart(fig_sunburst, use_container_width=True)

# ============================================================================
# TAB 5: ANÁLISIS TEMPORAL
# ============================================================================

with tab5:
    # Compras por Mes
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
        title="Evolución de Compras por Mes",
        template='plotly_dark',
        height=400,
        xaxis_title="Mes",
        yaxis_title="Cantidad de Compras"
    )
    st.plotly_chart(fig_temporal, use_container_width=True)
    
    # Análisis por Año (si hay múltiples años)
    if len(df_filtered['año'].unique()) > 1:
        col1, col2 = st.columns(2)
        
        with col1:
            año_counts = df_filtered['año'].value_counts().sort_index()
            fig_año = go.Figure(go.Bar(
                x=año_counts.index,
                y=año_counts.values,
                marker=dict(color='#00d9ff'),
                text=año_counts.values,
                textposition='outside'
            ))
            fig_año.update_layout(
                title="Clientes por Año",
                template='plotly_dark',
                height=400
            )
            st.plotly_chart(fig_año, use_container_width=True)
        
        with col2:
            gasto_año = df_filtered.groupby('año')['gasto_total'].sum().sort_index()
            fig_gasto_año = go.Figure(go.Bar(
                x=gasto_año.index,
                y=gasto_año.values,
                marker=dict(color='#00ff88'),
                text=[f'${x:,.0f}' for x in gasto_año.values],
                textposition='outside'
            ))
            fig_gasto_año.update_layout(
                title="Gasto Total por Año",
                template='plotly_dark',
                height=400
            )
            st.plotly_chart(fig_gasto_año, use_container_width=True)

# ============================================================================
# TABLA DE DATOS
# ============================================================================

st.divider()
st.subheader("Datos Detallados")

# Mostrar diferentes columnas si es cliente individual
if cliente_seleccionado:
    columnas_mostrar = [
        'customer_id', 'segmento_nombre', 'estrategia', 'age', 
        'gasto_total', 'probabilidad_compra', 'total_compras',
        'canal_recomendado', 'frecuencia_contacto', 'nivel_descuento',
        'ultima_compra', 'dias_desde_ultima_compra', 'precio_promedio'
    ]
else:
    columnas_mostrar = [
        'customer_id', 'segmento_nombre', 'estrategia', 'age', 
        'gasto_total', 'probabilidad_compra', 'total_compras',
        'canal_recomendado', 'frecuencia_contacto', 'nivel_descuento'
    ]

# Filtrar solo columnas que existen
columnas_existentes = [col for col in columnas_mostrar if col in df_filtered.columns]

df_display = df_filtered[columnas_existentes].copy()

# Formatear columnas
if 'gasto_total' in df_display.columns:
    df_display['gasto_total'] = df_display['gasto_total'].apply(lambda x: f"${x:,.2f}")
if 'probabilidad_compra' in df_display.columns:
    df_display['probabilidad_compra'] = df_display['probabilidad_compra'].apply(lambda x: f"{x:.2%}")
if 'precio_promedio' in df_display.columns:
    df_display['precio_promedio'] = df_display['precio_promedio'].apply(lambda x: f"${x:,.2f}")

# Mostrar tabla
st.dataframe(
    df_display.head(100),
    use_container_width=True,
    height=400
)

# ============================================================================
# EXPORTAR DATOS
# ============================================================================

st.divider()
col1, col2, col3 = st.columns([2, 1, 1])

with col1:
    st.subheader("Exportar Datos Filtrados")

with col2:
    csv = df_filtered.to_csv(index=False)
    st.download_button(
        label="Descargar CSV",
        data=csv,
        file_name=f"datos_filtrados_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
        use_container_width=True
    )

with col3:
    st.metric("Registros", f"{len(df_filtered):,}")

# ============================================================================
# FOOTER
# ============================================================================

st.divider()
st.markdown("""
<div style='text-align: center; color: #888; padding: 2rem;'>
    <p>Dashboard Prescriptivo | Powered by PySpark + Streamlit + Plotly</p>
    <p style='font-size: 0.8rem;'>Análisis en tiempo real de estrategias comerciales</p>
</div>
""", unsafe_allow_html=True)