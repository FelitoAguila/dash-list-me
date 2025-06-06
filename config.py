import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Configuración de MongoDB
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB_LIST_ME_TEST = 'ListMe-test'
MONGO_DB_LIST_ME = 'ListMe'
MONGO_COLLECTION_LISTS = 'lists'

# Configuración para métricas de suscripciones
MONGO_DB_USERS = 'Users'
MONGO_COLLECTION_SUBSCRIPTIONS = 'subscriptions'