import plotly.graph_objs as go
import plotly.express as px
import pandas as pd

def active_users_chart(df, view):
    fig = go.Figure()
    # Active Users
    fig.add_scatter(x=df["date"], y=df["total_users"], mode='lines+markers', name='Total Users', fill='tozeroy',
                    line=dict(color="#2C16AD"),marker=dict(size=4, symbol='circle'))
    # Exitosos
    fig.add_scatter(x=df["date"], y=df["successful_users"],mode='lines+markers', name='Successful Users', fill='tozeroy',
                    line=dict(color="#11B911"), marker=dict(size=4, symbol='circle'))
    
    # Fallidos
    fig.add_scatter(x=df["date"], y=df["failed_users"],mode='lines+markers', name='Failed Users', fill='tozeroy',
                    line=dict(color="#B91111"), marker=dict(size=4, symbol='circle'))
    
    #fig.update_xaxes(type='category')
    # Estética general
    fig.update_layout(title=f"{view} Total Active Users", yaxis_title="Users", xaxis_title="date",
                        yaxis_tickformat=',', title_x=0.5)
    return fig

def new_users_chart(df, view):
    fig = go.Figure()
    # Active Users
    fig.add_scatter(x=df["date"], y=df["total_users"], mode='lines+markers', name='Total New Users', fill='tozeroy',
                    line=dict(color="#2C16AD"),marker=dict(size=4, symbol='circle'))
    # Exitosos
    fig.add_scatter(x=df["date"], y=df["successful_users"],mode='lines+markers', name='Successful New Users', fill='tozeroy',
                    line=dict(color="#11B911"), marker=dict(size=4, symbol='circle'))
    
    # Fallidos
    fig.add_scatter(x=df["date"], y=df["failed_users"],mode='lines+markers', name='Failed New Users', fill='tozeroy',
                    line=dict(color="#B91111"), marker=dict(size=4, symbol='circle'))

    #fig.update_xaxes(type='category')
    # Estética general
    fig.update_layout(title=f"{view} Total Active New Users", yaxis_title="Users", xaxis_title="date",
                        yaxis_tickformat=',', title_x=0.5)
    return fig


def lists_chart(df, view):
    fig = go.Figure()
    # Active Users
    fig.add_scatter(x=df["date"], y=df["total_lists"], mode='lines+markers', name='Total Lists', fill='tozeroy',
                    line=dict(color="#2C16AD"),marker=dict(size=4, symbol='circle'))
    # Exitosos
    fig.add_scatter(x=df["date"], y=df["created_lists"],mode='lines+markers', name='Successful Lists', fill='tozeroy',
                    line=dict(color="#11B911"), marker=dict(size=4, symbol='circle'))
    
    # Fallidos
    fig.add_scatter(x=df["date"], y=df["failed_lists"],mode='lines+markers', name='Failed Lists', fill='tozeroy',
                    line=dict(color="#B91111"), marker=dict(size=4, symbol='circle'))
    
    #fig.update_xaxes(type='category')
    # Estética general
    fig.update_layout(title=f"{view} Lists", yaxis_title="Count", xaxis_title="date",
                        yaxis_tickformat=',', title_x=0.5)
    return fig

def users_percentage_chart(df, view):
    # Calculate percentage of new users
    df = df.copy()
    df['new_users_percentage'] = round((df['new_users'] / df['count'] * 100).fillna(0), 2)
    fig = go.Figure()
    fig.add_scatter(x=df["date"], y=df["new_users_percentage"],mode='lines+markers', name='Percentage', fill='tozeroy',
                    line=dict(color="#6BC26B"), marker=dict(size=4, symbol='circle'))
    # Estética general
    fig.update_layout(title=f'Percentage of New Users relative to Total {view} Active Users', yaxis_title="Percentage", xaxis_title="date",
                        yaxis_tickformat=',', title_x=0.5)
    return fig

def notified_chart(df, view):
    fig = go.Figure()
    # Notified
    fig.add_scatter(x=df["date"], y=df["notified_users"], mode='lines+markers', name='Notified Users', fill='tozeroy',
                    line=dict(color="#2C16AD"),marker=dict(size=4, symbol='circle'))
    
    #fig.update_xaxes(type='category')
    # Estética general
    fig.update_layout(title=f"{view} Notified Users", yaxis_title="Users", xaxis_title="date",
                        yaxis_tickformat=',', title_x=0.5)
    return fig

def users_by_country (data, countries, view):
    data['country'] = countries[0]
    filtered = data[data["country"].isin(countries)]
    fig = px.area(filtered, x="date", y="total_users", color="country", title=f"{view} Active Users")
    fig.update_traces(mode='lines+markers', marker=dict(size=4, symbol='circle'))
    fig.update_xaxes(type='category')
    fig.update_layout(yaxis_title="Users", xaxis_title="date", yaxis_tickformat=',', title_x=0.5)
    return fig

def lists_by_country (data, countries, view):
    data['country'] = countries[0]
    filtered = data[data["country"].isin(countries)]
    fig = px.area(filtered, x="date", y="created_lists", color="country", title=f"{view} Successful")
    fig.update_traces(mode='lines+markers', marker=dict(size=4, symbol='circle'))
    fig.update_xaxes(type='category')
    fig.update_layout(yaxis_title="Lists", xaxis_title="date", yaxis_tickformat=',', title_x=0.5)
    return fig

def dau_mau_ratio_chart(data, countries, title="DAU/MAU Ratio"):
    """
    Crea gráfico de línea para el ratio DAU/MAU por país
    
    Args:
        data: DataFrame con columnas year_month, country, dau_mau_ratio
        countries: Lista de países seleccionados
        title: Título del gráfico
    """
    
    if data.empty:
        fig = go.Figure()
        fig.add_annotation(text="No hay datos disponibles para el período seleccionado", xref="paper", yref="paper",
                            x=0.5, y=0.5, showarrow=False)
        return fig
    
    # Filtrar por países seleccionados
    if countries:
        data_filtered = data[data['country'].isin(countries)]
    else:
        data_filtered = data
    
    if data_filtered.empty:
        fig = go.Figure()
        fig.add_annotation(text="No hay datos para los países seleccionados", xref="paper", yref="paper",
                            x=0.5, y=0.5, showarrow=False)
        return fig
    
    # Crear gráfico de líneas
    fig = px.line(data_filtered, x='year_month', y='dau_mau_ratio', color='country', title=title,
                labels={'year_month': 'Mes', 'dau_mau_ratio': 'Ratio DAU/MAU', 'country': 'País'}, markers=True)
    
    # Configurar layout
    fig.update_layout(xaxis_title="Mes", yaxis_title="Ratio DAU/MAU", hovermode='x unified', 
                      legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
    
    # Formato del hover
    fig.update_traces(
        hovertemplate='<b>%{fullData.name}</b><br>' +
                      'Mes: %{x}<br>' +
                      'Ratio DAU/MAU: %{y:.3f}<br>' +
                      '<extra></extra>'
    )    
    fig.update_xaxes(type='category')
    return fig

def funnel_chart(data):
    """
    Crea un gráfico de barras anidadas con Plotly para mostrar usuarios notificados y usuarios que crearon listas.

    Args:
        data: DataFrame con columnas 'date', 'notified_users' y 'total_users'.

    Returns:
        fig: Objeto de figura de Plotly.
    """
    total_notified = data['notified_users'].sum()
    total_successful_users = data['total_users'].sum()

    # Calcular porcentajes respecto a total_notified
    if total_notified > 0:  # Evitar división por cero
        percent_notified = 100  # Siempre 100% para notificados
        percent_successful = (total_successful_users / total_notified) * 100
    else:
        percent_notified, percent_successful = 0, 0

    # Crear la figura
    fig = go.Figure()

    # Agregar barra para total_notified (barra más ancha, fondo)
    fig.add_trace(go.Bar(
        x=[""],
        y=[total_notified],
        name=f'Usuarios notificados ({percent_notified:.1f}%)',
        marker_color="#00008B",  # Azul oscuro
        width=0.4,  # Ancho mayor para la barra de fondo
        hovertemplate='Usuarios notificados: %{y} (%{customdata:.1f}%)<extra></extra>',
        customdata=[percent_notified]  # Datos personalizados para el hover
        # text=[f'{total_notified} ({percent_notified:.1f}%)'],
        # textposition='outside',  # Texto fuera para mayor claridad
        # textfont=dict(size=12, color="#00008B")
    ))

    # Agregar barra para successful_users (barra más angosta, superpuesta)
    fig.add_trace(go.Bar(
        x=[""],
        y=[total_successful_users],
        name=f'Usuarios de Listas ({percent_successful:.1f}%)',
        marker_color="#1E90FF",  # Azul claro
        width=0.4,  # Ancho menor para mostrar que está "dentro"
        hovertemplate='Usuarios de Listas: %{y} (%{customdata:.1f}%)<extra></extra>',
        customdata=[percent_successful]  # Datos personalizados para el hover
        #text=[f'{total_successful_users} ({percent_successful:.1f}%)'],
        #textposition='inside',  # Texto dentro para enfatizar contención
        #textfont=dict(size=12, color="#FFFFFF")
    ))

    # Actualizar el diseño del gráfico
    fig.update_layout(
        title=dict(
            text='Funnel Usuarios ListMe',
            x=0.5,  # Centrar título
            xanchor='center'
        ),
        yaxis_title='Cantidad de Usuarios',
        barmode='overlay',  # Superponer barras en lugar de apilar
        showlegend=True,
        font=dict(family='Roboto, sans-serif', size=12, color='#333'),
        plot_bgcolor='#FFFFFF',  # Fondo blanco
        paper_bgcolor='#FFFFFF',
        margin=dict(l=50, r=50, t=80, b=50),
        uniformtext_minsize=10,
        uniformtext_mode='hide',
        # Ajustar el eje Y para mejor visualización
        yaxis=dict(
            showgrid=True,
            gridcolor='rgba(0, 0, 0, 0.1)',
            zeroline=False
        ),
        xaxis=dict(
            showgrid=False,
            zeroline=False
        )
    )
    return fig
