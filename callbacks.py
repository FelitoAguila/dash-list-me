import pymongo
from config import MONGO_URI, MONGO_DB_LIST_ME, MONGO_COLLECTION_LISTS, MONGO_DB_LIST_ME_TEST
from dash import Input, Output, html, dcc
import dash_bootstrap_components as dbc
from datetime import datetime
from get_data import (get_daily_data, group_monthly_data, get_new_user_reminder_metrics_by_day, 
                      calculate_total_metrics, get_dau_mau_ratio_data, parse_date_range)

from charts import (active_users_chart, lists_chart, new_users_chart)

client = pymongo.MongoClient(MONGO_URI)
db_LME_test = client[MONGO_DB_LIST_ME_TEST]
collection_lme_test = db_LME_test[MONGO_COLLECTION_LISTS]


# Cache para gráficos (datos que cambian según filtros)
_charts_cache = {}

# Calcular métricas una sola vez al importar el módulo
TOTAL_METRICS = calculate_total_metrics(collection_lme_test)

def get_chart_data(view, start_date, end_date):
    """Obtiene datos para gráficos con cache"""
    cache_key = f"{view}_{start_date}_{end_date}"
    
    if cache_key not in _charts_cache:
        print(f"Obteniendo datos para gráficos: {view} desde {start_date} hasta {end_date}")
        start, end = parse_date_range(start_date, end_date)
        data = get_daily_data(collection_lme_test, start, end)
        if view == 'Daily':
            _charts_cache[cache_key] = data
        elif view == 'Monthly':
            _charts_cache[cache_key] = group_monthly_data(data)
    
    return _charts_cache[cache_key]

# Cache para datos de ratio
_ratio_cache = {}

def get_ratio_data(start_date, end_date, countries=None):
    """Obtiene datos de ratio DAU/MAU con cache"""
    cache_key = f"ratio_{start_date}_{end_date}_{str(sorted(countries) if countries else [])}"
    
    if cache_key not in _ratio_cache:
        print(f"Obteniendo datos de ratio DAU/MAU para {countries}")
        _ratio_cache[cache_key] = get_dau_mau_ratio_data(collection_lme_test, start_date, end_date, countries)
    
    return _ratio_cache[cache_key]

def register_callbacks(app):
    
    # Callback SOLO para métricas - valores fijos que NO cambian
    @app.callback(
        [
            Output('total_lists_attempted', 'children'),
            Output('total_lists_created', 'children'),
            Output('total_failed_lists', 'children'),
            Output('total_users', 'children'),
            Output('total_successful_users', 'children'),
            Output('total_failed_users', 'children'),
        ],
        [Input('start_date_picker', 'date')]  # Solo necesitamos un trigger, pero los valores no dependen de esto
    )
    def update_total_metrics(start_date):
        """Retorna métricas totales fijas - NO cambian con los filtros"""
        return (
            TOTAL_METRICS['total_lists_attempted'],
            TOTAL_METRICS['total_lists_created'], 
            TOTAL_METRICS['total_failed_lists'],
            TOTAL_METRICS['total_users'], 
            TOTAL_METRICS['total_successful_users'], 
            TOTAL_METRICS['total_failed_users']
        )
    
    # Callback para el contenido de las pestañas
    @app.callback(
        Output('tab-content', 'children'),
        [Input('main-tabs', 'value'), 
         Input('view_selector', 'value'),
         Input('start_date_picker', 'date'), 
         Input('end_date_picker', 'date')]
    )
    def render_tab_content(active_tab, view, start_date, end_date):
        """Renderiza el contenido según la pestaña seleccionada"""
        
        if active_tab == 'general':
            return html.Div([
                # Gráficos - Vista General
                html.Div([
                    html.Div([html.H3(f"{view} Active Users", style={'textAlign': 'center'}), dcc.Graph(id='active_users_fig')], style={'flex': '1', 'minWidth': '45%', 'margin': '10px', 'border': '1px solid #ddd', 'borderRadius': '5px', 'padding': '10px'}),
                    html.Div([html.H3(f"{view} Lists", style={'textAlign': 'center'}), dcc.Graph(id='lists_fig')], style={'flex': '1', 'minWidth': '45%', 'margin': '10px', 'border': '1px solid #ddd', 'borderRadius': '5px', 'padding': '10px'}),
                    html.Div([html.H3(f"{view} New Users", style={'textAlign': 'center'}), dcc.Graph(id='new_users_fig')], style={'flex': '1', 'minWidth': '45%', 'margin': '10px', 'border': '1px solid #ddd', 'borderRadius': '5px', 'padding': '10px'}),
                ], style={'display': 'flex', 'flexWrap': 'wrap', 'justifyContent': 'space-around'})
            ])
        elif active_tab == 'países':
            # Obtener países disponibles para el período seleccionado
            start = datetime.strptime(start_date[:10], '%Y-%m-%d')
            end = datetime.strptime(end_date[:10], '%Y-%m-%d')
            start_date_str = start.strftime('%Y-%m-%d')
            end_date_str = end.strftime('%Y-%m-%d')
            
            data = get_chart_data(view, start_date_str, end_date_str)
            countries = data["country"].unique() if len(data) > 0 else []
            
            return html.Div([
                html.Label("Selecciona país(es):"),
                dcc.Dropdown(
                    id="country_dropdown",
                    options=[{"label": c, "value": c} for c in countries],
                    value=["Argentina"] if "Argentina" in countries else [countries[0]] if len(countries) > 0 else [],
                    multi=True
                ),
                html.Div([html.H3(f"{view} Active Users", style={'textAlign': 'center'}), dcc.Graph(id='dau_by_country')], style={'flex': '1', 'minWidth': '45%', 'margin': '10px', 'border': '1px solid #ddd', 'borderRadius': '5px', 'padding': '10px'}),
                html.Div([html.H3(f"{view} New Users", style={'textAlign': 'center'}), dcc.Graph(id='new_users_by_country')], style={'flex': '1', 'minWidth': '45%', 'margin': '10px', 'border': '1px solid #ddd', 'borderRadius': '5px', 'padding': '10px'}),
                html.Div([html.H3("DAU/MAU Ratio por Mes", style={'textAlign': 'center'}), dcc.Graph(id='dau_mau_ratio_chart')], style={'margin': '20px 10px', 'border': '1px solid #ddd', 'borderRadius': '5px', 'padding': '10px'})
            ])
        return html.Div([html.P("Selecciona una pestaña para ver el contenido.")])
    
    # Callback para gráficos generales - SÍ cambian con filtros
    @app.callback(
        [
            Output('active_users_fig', 'figure'),
            Output('lists_fig', 'figure'),
            Output('new_users_fig', 'figure')
        ],
        [
            Input('start_date_picker', 'date'), 
            Input('end_date_picker', 'date'),
            Input('view_selector', 'value')
        ]
    )
    def update_general_charts(start_date, end_date, view):
        """Actualiza gráficos generales según filtros seleccionados"""
        # Parsear fechas
        start = datetime.strptime(start_date[:10], '%Y-%m-%d')
        end = datetime.strptime(end_date[:10], '%Y-%m-%d')
        start_date_str = start.strftime('%Y-%m-%d')
        end_date_str = end.strftime('%Y-%m-%d')
        
        # Obtener datos para el período seleccionado
        data = get_chart_data(view, start_date_str, end_date_str)
        
        # Generar gráficos
        active_users_fig = active_users_chart(data, view)
        lists_fig = lists_chart(data, view)
        new_users_fig = new_users_chart(data, view)
        
        return active_users_fig, lists_fig, new_users_fig