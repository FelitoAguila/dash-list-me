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
    
    fig.update_xaxes(type='category')
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

    fig.update_xaxes(type='category')
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
    
    fig.update_xaxes(type='category')
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
