from dash import dcc, html
from datetime import datetime
import pytz
from get_data import get_daily_data

timezone = pytz.timezone('America/Argentina/Buenos_Aires')



def serve_layout():
    # Layout
    return html.Div([
        # Header
        html.Div([
            html.H1("Dashboard- ListMe", 
                    style={"color": "#0AB84D", "marginBottom": "10px"}),
            html.Hr(style={"margin": "0 0 20px 0"}),
        ], style={"textAlign": "center", "paddingTop": "20px"}),

        # Selector de fechas
        html.Div([
            html.Div([
                html.Label("Fecha de inicio:"),
                dcc.DatePickerSingle(
                    id='start_date_picker',
                    date=datetime(2025, 6, 1),
                    display_format='YYYY-MM-DD',
                    min_date_allowed=datetime(2025, 6, 1).date(),
                    max_date_allowed=datetime.now(timezone).date(),
                    style={'marginBottom': '10px'}
                ),
            ], style={'margin': '10px', 'flex': '1'}),

            html.Div([
                html.Label("Fecha de fin:"),
                dcc.DatePickerSingle(
                    id='end_date_picker',
                    date=datetime.now(timezone),
                    display_format='YYYY-MM-DD',
                    min_date_allowed=datetime(2025, 6, 1).date(),
                    max_date_allowed=datetime.now(timezone).date(),
                    style={'marginBottom': '10px'}
                ),
            ], style={'margin': '10px', 'flex': '1'}),

            html.Div([
                html.Label("Vista:"),
                dcc.RadioItems(
                    id='view_selector',
                    options=[
                        {'label': 'Diario', 'value': 'Daily'},
                        {'label': 'Mensual', 'value': 'Monthly'}
                    ],
                    value='Daily',
                    style={'display': 'flex', 'gap': '10px'}
                ),
            ], style={'margin': '10px', 'flex': '1'})
        ], style={'display': 'flex', 'justifyContent': 'space-between', 'margin': '20px'}),

        # Tarjetas de métricas
        html.Div([
            html.Div([html.H3("Total Intentos"), html.H2(id='total_lists_attempted', children='0')], className='metric-card'),
            html.Div([html.H3("Listas Creadas"), html.H2(id='total_lists_created', children='0')], className='metric-card'),
            html.Div([html.H3("Listas Fallidas"), html.H2(id='total_failed_lists', children='0')], className='metric-card'),
            html.Div([html.H3("Total Usuarios"), html.H2(id='total_users', children='0')], className='metric-card'),
            html.Div([html.H3("Usuarios Exitosos"), html.H2(id='total_successful_users', children='0')], className='metric-card'),
            html.Div([html.H3("Usuarios Fallidos"), html.H2(id='total_failed_users', children='0')], className='metric-card'),
        ], style={'display': 'flex', 'flexWrap': 'wrap', 'justifyContent': 'space-around', 'gap': '15px', 'margin': '20px'}),

        # Pestañas para las vistas
        html.Div([
            dcc.Tabs(id="main-tabs", value="general", children=[
                dcc.Tab(label="Vista General", value="general"),
                dcc.Tab(label="Análisis por países", value="países"),
            ], style={'marginBottom': '20px'}),

            # Contenido de las pestañas
            html.Div(id="tab-content")
        ], style={'margin': '20px'}),
], style={'fontFamily': 'Arial, sans-serif', 'margin': '0 auto', 'maxWidth': '1400px', 'padding': '20px'})
