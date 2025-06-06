from dotenv import load_dotenv
import os
import dash
from dash import dcc, html, Input, Output
import plotly.express as px
import pandas as pd
import pymongo
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import plotly.graph_objects as go
from layout import serve_layout
from callbacks import register_callbacks
from dash import Dash
import dash_bootstrap_components as dbc

# Initialize Dash app
app = Dash(__name__, meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}], 
           external_stylesheets=[dbc.themes.BOOTSTRAP], suppress_callback_exceptions=True)

# Layout
app.layout = serve_layout()
register_callbacks(app)

server = app.server  # para que Gunicorn pueda encontrarlo

# Run the app
if __name__ == '__main__':
    app.run(debug=True, port = 8050)