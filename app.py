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
import dash_auth
import hashlib
from flask import request

# Cargar variables de entorno desde .env
load_dotenv()

# Obtener usuario y contraseña hasheada desde el entorno
USERNAME = os.getenv("DASH_USER")
PASSWORD_HASH = os.getenv("DASH_PASS_HASH")

# Función para hashear contraseñas
def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

# Autenticador personalizado
class HashedAuth(dash_auth.BasicAuth):
    def is_authorized(self):
        auth = request.authorization  # <-- Usar flask.request directamente
        if not auth:
            return False
        return auth.username == USERNAME and hash_password(auth.password) == PASSWORD_HASH

# Initialize Dash app
app = Dash(__name__, meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}], 
           external_stylesheets=[dbc.themes.BOOTSTRAP], suppress_callback_exceptions=True)

# Instanciar autenticación con diccionario dummy
auth = HashedAuth(app, {'dummy': 'dummy'})

# Layout
app.layout = serve_layout()
register_callbacks(app)

server = app.server  # para que Gunicorn pueda encontrarlo

# Run the app
if __name__ == '__main__':
    app.run(debug=False, port = 8050)
