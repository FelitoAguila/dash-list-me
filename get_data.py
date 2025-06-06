from datetime import datetime, timedelta, time
import pandas as pd
from config import MONGO_URI, MONGO_DB_LIST_ME, MONGO_COLLECTION_LISTS, MONGO_DB_LIST_ME_TEST
import pytz

def parse_date_range(start_date_str: str, end_date_str: str) -> tuple[datetime, datetime]:
    """
    Convierte las fechas desde el frontend a objetos datetime con zona horaria
    y aplica la lógica especial para el 2025-05-22.
    """
    # Tu zona horaria de trabajo
    tz = pytz.timezone('America/Argentina/Buenos_Aires')

    # Fecha mínima válida
    min_allowed_date = datetime(2025, 5, 22, 17, 30, tzinfo=tz)
    
    # Convertimos a date (sin hora aún)
    start_date_raw = datetime.fromisoformat(start_date_str).date()
    end_date_raw = datetime.fromisoformat(end_date_str).date()

    # Lógica para la hora
    if start_date_raw <= min_allowed_date.date():
        start_dt = min_allowed_date
    else:
        start_dt = datetime.combine(start_date_raw, time.min, tz)

    if end_date_raw <= min_allowed_date.date():
        end_dt = min_allowed_date
    else:
        end_dt = datetime.combine(end_date_raw, time.max, tz)
    return start_dt, end_dt

def get_daily_data(collection, start_date, end_date):
    """
    Extrae la data de la Mongo    
    """
    # Asegurar que start_date y end_date tengan zona horaria
    if start_date.tzinfo is None or end_date.tzinfo is None:
        raise ValueError("start_date y end_date deben tener zona horaria")
    
    # Hacer end_date inclusivo sumando un día
    end_date_inclusive = end_date + timedelta(days=1)

    # Convertir start_date y end_date a Unix timestamp en segundos
    start_timestamp = start_date.timestamp()
    end_timestamp = end_date_inclusive.timestamp()

    # Pipeline de agregación
    pipeline = [
        # 1. Filtrar por rango de fechas en created_at (Unix timestamp)
        {
            "$match": {
                "created_at": {
                    "$gte": start_timestamp,
                    "$lte": end_timestamp
                }
            }
        },
        # 2. Agrupar por día (convirtiendo created_at a fecha)
        {
            "$group": {
                "_id": {
                    "$dateToString": {
                        "format": "%Y-%m-%d",
                        "date": {"$toDate": {"$multiply": ["$created_at", 1000]}},  # Convertir segundos a milisegundos
                        "timezone": "America/Argentina/Buenos_Aires"
                    }
                },
                # Conteos de listas
                "total_lists": {"$sum": 1},
                "failed_lists": {
                    "$sum": {
                        "$cond": [{"$eq": ["$status", "error"]}, 1, 0]
                    }
                },
                # Conteos de usuarios únicos
                "all_users": {"$addToSet": "$user_id"},
                "failed_users_set": {
                    "$addToSet": {
                        "$cond": [{"$eq": ["$status", "error"]}, "$user_id", None]
                    }
                },
                "successful_users_set": {
                    "$addToSet": {
                        "$cond": [{"$ne": ["$status", "error"]}, "$user_id", None]
                    }
                }
            }
        },
        # 3. Calcular created_lists y conteos finales de usuarios
        {
            "$project": {
                "date": "$_id",
                "total_lists": 1,
                "failed_lists": 1,
                "created_lists": {
                    "$subtract": ["$total_lists", "$failed_lists"]
                },
                "total_users": {"$size": "$all_users"},
                "failed_users": {
                    "$size": {
                        "$filter": {
                            "input": "$failed_users_set",
                            "cond": {"$ne": ["$$this", None]}
                        }
                    }
                },
                "successful_users": {
                    "$size": {
                        "$filter": {
                            "input": "$successful_users_set",
                            "cond": {"$ne": ["$$this", None]}
                        }
                    }
                },
                "_id": 0
            }
        },
        # 4. Ordenar por fecha
        {
            "$sort": {"date": 1}
        }
    ]
        
    # Ejecutar el pipeline y obtener resultados
    results = list(collection.aggregate(pipeline))

    # Convertir a DataFrame
    df = pd.DataFrame(results)

    # Si no hay resultados, devolver DataFrame vacío con columnas correctas
    if df.empty:
        return pd.DataFrame(columns=[
            "date", "total_lists", "failed_lists", "created_lists",
            "total_users", "failed_users", "successful_users"
        ])
    
    # Asegurar que las columnas estén en el orden correcto
    df = df[["date", "total_lists", "failed_lists", "created_lists",
             "total_users", "failed_users", "successful_users"]]

    # Convertir la columna date a datetime
    #df["date"] = pd.to_datetime(df["date"])
    return df
    

def group_monthly_data(df):
    monthly_df = df.copy()
    monthly_df['date'] = pd.to_datetime(monthly_df['date'])
    
    # Definir las columnas que queremos agregar
    possible_columns = ["total_lists", "failed_lists", "created_lists",
             "total_users", "failed_users", "successful_users"]
    
    # Filtrar solo las columnas que existen en el DataFrame
    agg_dict = {col: 'sum' for col in possible_columns if col in monthly_df.columns}
    
    # Realizar la agregación
    monthly_df = monthly_df.groupby(monthly_df['date'].dt.to_period('M')).agg(agg_dict).reset_index()
    monthly_df['date'] = monthly_df['date'].astype(str)
    return monthly_df

def get_new_user_reminder_metrics_by_day(start_date: datetime, end_date: datetime, collection) -> pd.DataFrame:
    """
    Obtiene métricas por día de usuarios nuevos en su primer día de uso:
    - total de usuarios nuevos
    - listas totales, fallidas y exitosas
    - usuarios fallidos y exitosos (en su primer día)
    
    Args:
        start_date (datetime): Fecha de inicio con zona horaria.
        end_date (datetime): Fecha de fin con zona horaria.
        collection: Colección de MongoDB conectada.
    
    Returns:
        pd.DataFrame: DataFrame con columnas:
            ['date', 'total_users', 'total_reminders', 'failed_reminders',
             'created_reminders', 'failed_users', 'successful_users']
    """
    if start_date.tzinfo is None or end_date.tzinfo is None:
        raise ValueError("start_date y end_date deben tener zona horaria")

    end_date_inclusive = end_date + timedelta(days=1)
    start_timestamp = start_date.timestamp()
    end_timestamp = end_date_inclusive.timestamp()

    pipeline = [
        # Paso 1: determinar la primera aparición de cada usuario
        {
            "$group": {
                "_id": "$user_id",
                "first_seen": {"$min": "$created_at"}
            }
        },
        # Paso 2: proyectar la fecha de primera vez con zona horaria
        {
            "$project": {
                "user_id": "$_id",
                "first_seen": 1,
                "first_seen_date": {
                    "$dateToString": {
                        "format": "%Y-%m-%d",
                        "date": {"$toDate": {"$multiply": ["$first_seen", 1000]}},
                        "timezone": "America/Argentina/Buenos_Aires"
                    }
                },
                "_id": 0
            }
        },
        # Paso 3: filtrar solo usuarios nuevos dentro del rango de fechas
        {
            "$match": {
                "first_seen": {"$gte": start_timestamp, "$lte": end_timestamp}
            }
        },
        # Paso 4: guardar temporalmente los usuarios nuevos en su primer día
        {
            "$lookup": {
                "from": collection.name,
                "let": {"user_id": "$user_id", "first_seen": "$first_seen"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$user_id", "$$user_id"]},
                                    {"$eq": [
                                        {
                                            "$dateToString": {
                                                "format": "%Y-%m-%d",
                                                "date": {"$toDate": {"$multiply": ["$created_at", 1000]}},
                                                "timezone": "America/Argentina/Buenos_Aires"
                                            }
                                        },
                                        {
                                            "$dateToString": {
                                                "format": "%Y-%m-%d",
                                                "date": {"$toDate": {"$multiply": ["$$first_seen", 1000]}},
                                                "timezone": "America/Argentina/Buenos_Aires"
                                            }
                                        }
                                    ]}
                                ]
                            }
                        }
                    }
                ],
                "as": "first_day_lists"
            }
        },
        # Paso 5: calcular métricas por usuario
        {
            "$project": {
                "first_seen_date": 1,
                "has_failed": {
                    "$anyElementTrue": {
                        "$map": {
                            "input": "$first_day_lists",
                            "as": "r",
                            "in": {"$eq": ["$$r.status", "error"]}
                        }
                    }
                },
                "lists": "$first_day_lists"
            }
        },
        # Paso 6: descomponer por fecha
        {
            "$group": {
                "_id": "$first_seen_date",
                "total_users": {"$sum": 1},
                "total_lists": {"$sum": {"$size": "$lists"}},
                "failed_lists": {
                    "$sum": {
                        "$size": {
                            "$filter": {
                                "input": "$lists",
                                "cond": {"$eq": ["$$this.status", "error"]}
                            }
                        }
                    }
                },
                "failed_users": {"$sum": {"$cond": ["$has_failed", 1, 0]}},
                "successful_users": {"$sum": {"$cond": ["$has_failed", 0, 1]}}
            }
        },
        # Paso 7: calcular exitosos y proyectar
        {
            "$project": {
                "date": "$_id",
                "total_users": 1,
                "total_lists": 1,
                "failed_lists": 1,
                "created_lists": {
                    "$subtract": ["$total_lists", "$failed_lists"]
                },
                "failed_users": 1,
                "successful_users": 1,
                "_id": 0
            }
        },
        {
            "$sort": {"date": 1}
        }
    ]

    results = list(collection.aggregate(pipeline))
    df = pd.DataFrame(results)

    if df.empty:
        return pd.DataFrame(columns=[
            "date", "total_users", "total_lists", "failed_lists",
            "created_lists", "failed_users", "successful_users"
        ])

    return df[["date", "total_users", "total_lists", "failed_lists",
            "created_lists", "failed_users", "successful_users"]]

# Para formatear los datos históricos
def format_number_smart(number):
    """Formato inteligente: compacto para números muy grandes, completo para otros"""
    if isinstance(number, (int, float)):
        if number >= 10000000:  # 10M o más -> formato compacto
            if number >= 1000000000:
                return f"{number/1000000000:.1f}B"
            elif number >= 1000000:
                return f"{number/1000000:.1f}M"
        else:  # Menos de 10M -> formato completo con puntos
            return f"{number:,.0f}".replace(',', '.')
    return str(number)

# MÉTRICAS TOTALES FIJAS - Se calculan una sola vez al iniciar la app
def calculate_total_metrics(collection):
    """Calcula métricas totales desde 2023-01-01 hasta hoy - SOLO SE EJECUTA UNA VEZ"""
    start_date = "2023-01-01"
    end_date = datetime.now().strftime('%Y-%m-%d')
    start, end = parse_date_range(start_date, end_date)
    
    print(f"Calculando métricas totales desde {start_date} hasta {end_date}...")
    
    # Obtener datos completos
    daily_data = get_daily_data(collection, start, end)
    daily_new_users_data = get_new_user_reminder_metrics_by_day(start, end, collection)

    # Calcular métricas totales
    total_lists_attempted = int(daily_data['total_lists'].sum())
    total_lists_created = int(daily_data['created_lists'].sum())
    total_failed_lists = int(daily_data['failed_lists'].sum())
    total_users = int(daily_new_users_data['total_users'].sum())
    total_successful_users = int(daily_new_users_data['successful_users'].sum())
    total_failed_users = int(daily_new_users_data['failed_users'].sum())
    
    print("Métricas totales calculadas exitosamente")
    
    return {
        'total_lists_attempted': format_number_smart(total_lists_attempted),
        'total_lists_created': format_number_smart(total_lists_created),
        'total_failed_lists': format_number_smart(total_failed_lists),
        'total_users': format_number_smart(total_users),
        'total_successful_users': format_number_smart(total_successful_users),
        'total_failed_users': format_number_smart(total_failed_users)
    }

def get_dau_mau_ratio_data(collection_dau, collection_mau, start_date, end_date, countries=None):
    ratio_data = []    
    return ratio_data