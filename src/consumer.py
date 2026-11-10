import json
import time
from datetime import datetime
from pathlib import Path
from confluent_kafka import Consumer
import psycopg2
import pandas as pd
import joblib
import os
from dotenv import load_dotenv


# ============================================================
# CONFIGURACIÓN
# ============================================================
BASE_DIR = Path(__file__).resolve().parent.parent
TOPIC = "happiness-events"
MODEL_PATH = BASE_DIR / "models" / "happiness_pipeline.joblib"

# 1. Cargar modelo
if not MODEL_PATH.exists():
    raise FileNotFoundError("No se encontró el modelo. Ejecuta train.py primero.")
pipeline = joblib.load(MODEL_PATH)
print("Modelo de ML cargado exitosamente.")


load_dotenv()

# 2. Conexión a PostgreSQL
try:
    conn = psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB", "happiness_db"),       
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", ""), 
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5433")
    )
    conn.autocommit = True
    cursor = conn.cursor()
    print("Conexión a PostgreSQL establecida.")
except Exception as e:
    print(f"❌ ERROR DETALLADO al conectar a PostgreSQL: {e}")
    raise SystemExit(1)

# 3. Configuración del Consumidor
consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "happiness-etl-team",
    "auto.offset.reset": "earliest",
})
consumer.subscribe([TOPIC])

# ============================================================
# INICIALIZACIÓN DE BASE DE DATOS
# ============================================================
def init_db():
    # 0. Tabla de Eventos Crudos (Requerimiento Paso 8)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_happiness_events (
            raw_event_id VARCHAR(50) PRIMARY KEY,
            raw_payload TEXT NOT NULL,
            received_at TIMESTAMP NOT NULL
        )
    """)
    # 1. Dimensión País
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_country (
            country_id SERIAL PRIMARY KEY,
            country_name VARCHAR(100) UNIQUE NOT NULL
        )
    """)
    # 2. Dimensión Fecha
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_date (
            date_id INT PRIMARY KEY, 
            year INT NOT NULL,
            month INT NOT NULL,
            day INT NOT NULL,
            quarter INT NOT NULL,
            day_of_week VARCHAR(20) NOT NULL
        )
    """)
    # 3. Tabla de Hechos y Tabla de Rechazados
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fact_predictions (
            prediction_id SERIAL PRIMARY KEY,
            raw_event_id VARCHAR(50) REFERENCES raw_happiness_events(raw_event_id), -- Vinculado al evento crudo
            country_id INT REFERENCES dim_country(country_id),
            date_id INT REFERENCES dim_date(date_id),
            gdp FLOAT, family FLOAT, health FLOAT, freedom FLOAT,
            generosity FLOAT, corruption FLOAT, actual_score FLOAT,
            predicted_score FLOAT, prediction_error FLOAT,
            event_timestamp TIMESTAMP, processed_timestamp TIMESTAMP,
            processing_delay_seconds FLOAT
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS rejected_happiness_events (
            id SERIAL PRIMARY KEY,
            event_id VARCHAR(50),
            raw_event TEXT,
            rejection_reason TEXT,
            rejected_at TIMESTAMP
        )
    """)
    print("Modelo Dimensional (Paso 8) validado/creado en PostgreSQL.")

# ============================================================
# FUNCIONES DE PROCESAMIENTO
# ============================================================
def validate_event(event):
    """Valida los campos del evento. Devuelve (bool, reason)."""
    required_fields = [
        "event_id", "country", "year", "gdp",
        "family", "health", "freedom", "generosity", 
        "corruption", "actual_happiness_score", "event_time"
    ]

    for field in required_fields:
        if field not in event:
            return False, f"Missing field: {field}"
        if event[field] in (None, ""):
            return False, f"Empty field: {field}"

    # Corregido: event_id viene como string ("E001") del productor
    if not isinstance(event["event_id"], str):
        return False, "event_id must be a string"
    if not isinstance(event["country"], str):
        return False, "country must be a string"
    if not isinstance(event["year"], int):
        return False, "year must be an int"
    if event["year"] not in [2015, 2016, 2017, 2018, 2019]:
        return False, "year must be between 2015 and 2019"

    # Validaciones numéricas relajadas para ajustarse al dataset real
    numeric_fields = ["gdp", "family", "health", "freedom", "generosity", "corruption", "actual_happiness_score"]
    for field in numeric_fields:
        if not isinstance(event[field], (int, float)) or isinstance(event[field], bool):
            return False, f"{field} must be numeric"

    try:
        datetime.fromisoformat(event["event_time"])
    except (ValueError, TypeError):
        return False, "event_time must be a valid ISO timestamp"

# Verificar duplicados en Postgres (CÓDIGO CORREGIDO)
    cursor.execute("SELECT 1 FROM fact_predictions WHERE raw_event_id = %s", (event["event_id"],))
    if cursor.fetchone():
        return False, f"Duplicate event_id: {event['event_id']}"

    return True, "Valid event"


def enrich_and_predict(event):
    """Mapea variables, hace inferencia y calcula métricas."""
    # Mapeo del contrato (Consumer) al esquema del Modelo (scikit-learn)
    df_input = pd.DataFrame([{
        "gdp_per_capita": event["gdp"],
        "social_support": event["family"],
        "health_life_expectancy": event["health"],
        "freedom": event["freedom"],
        "generosity": event["generosity"],
        "trust_corruption": event["corruption"]
    }])

    # Inferencia
    predicted_score = float(pipeline.predict(df_input)[0])
    actual_score = float(event["actual_happiness_score"])
    
    event_dt = datetime.fromisoformat(event["event_time"])
    processed_dt = datetime.now()

    enriched = dict(event)
    enriched.update({
        "predicted_score": predicted_score,
        "prediction_error": abs(actual_score - predicted_score),
        "processed_at": processed_dt.isoformat(timespec="seconds"),
        "processing_delay_seconds": (processed_dt - event_dt).total_seconds(),
    })
    return enriched



def persist_raw_event(event_id, raw_json_str):
    """Guarda el mensaje de Kafka intacto en la base de datos."""
    cursor.execute("""
        INSERT INTO raw_happiness_events (raw_event_id, raw_payload, received_at)
        VALUES (%s, %s, %s)
        ON CONFLICT (raw_event_id) DO NOTHING
    """, (event_id, raw_json_str, datetime.now().isoformat(timespec="seconds")))
    
    
# ============================================================
# FUNCIÓN DE PERSISTENCIA (ETL AL VUELO)
# ============================================================
def persist_valid(event):
    """Guarda en dimensiones primero, obtiene los IDs y luego guarda el hecho."""
    
    # 1. Procesar y guardar en dim_date
    event_dt = datetime.fromisoformat(event["event_time"])
    date_id = int(event_dt.strftime("%Y%m%d")) # Formato YYYYMMDD (Ej: 20261030)
    
    cursor.execute("""
        INSERT INTO dim_date (date_id, year, month, day, quarter, day_of_week)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (date_id) DO NOTHING
    """, (
        date_id, event_dt.year, event_dt.month, event_dt.day,
        (event_dt.month - 1) // 3 + 1, event_dt.strftime("%A")
    ))

    # 2. Procesar y guardar en dim_country (Obtener country_id)
    cursor.execute("""
        INSERT INTO dim_country (country_name)
        VALUES (%s)
        ON CONFLICT (country_name) DO NOTHING;
    """, (event["country"],))
    
    # Recuperar el ID del país (ya sea recién insertado o existente)
    cursor.execute("SELECT country_id FROM dim_country WHERE country_name = %s", (event["country"],))
    country_id = cursor.fetchone()[0]

    # 3. Insertar métricas en fact_predictions
    cursor.execute("""
        INSERT INTO fact_predictions (
            raw_event_id, country_id, date_id, 
            gdp, family, health, freedom, generosity, corruption, 
            actual_score, predicted_score, prediction_error, 
            event_timestamp, processed_timestamp, processing_delay_seconds
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        event["event_id"], country_id, date_id,
        event["gdp"], event["family"], event["health"], 
        event["freedom"], event["generosity"], event["corruption"],
        event["actual_happiness_score"], event["predicted_score"], 
        event["prediction_error"], event["event_time"], 
        event["processed_at"], event["processing_delay_seconds"]
    ))  

def persist_rejected(event, reason):
    event_id = event.get("event_id") if isinstance(event, dict) else None
    cursor.execute("""
        INSERT INTO rejected_happiness_events (event_id, raw_event, rejection_reason, rejected_at)
        VALUES (%s, %s, %s, %s)
    """, (
        event_id, json.dumps(event), reason, datetime.now().isoformat(timespec="seconds")
    ))

# ============================================================
# BUCLE PRINCIPAL
# ============================================================
def start_consumer():
    init_db()
    print(f"Esperando eventos de felicidad en el tópico '{TOPIC}'...\n")
    
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Error de Kafka: {msg.error()}")
                continue

            # Extraer el texto crudo del mensaje de Kafka
            raw_json_str = msg.value().decode("utf-8")
            
            try:
                raw_event = json.loads(raw_json_str)
            except json.JSONDecodeError:
                persist_rejected(raw_json_str, "Invalid JSON format")
                continue

            # 1. Validar
            is_valid, reason = validate_event(raw_event)
            
            if is_valid:
                # 2. Transformar / Predecir
                enriched_event = enrich_and_predict(raw_event)
                
                # 3. GUARDAR EVENTO CRUDO (Paso 8)
                persist_raw_event(enriched_event["event_id"], raw_json_str)
                
                # 4. Guardar evento exitoso (Modelo Dimensional)
                persist_valid(enriched_event)
                
                print(f"✅ Procesado: {enriched_event['event_id']} - {enriched_event['country']} "
                      f"| Real: {enriched_event['actual_happiness_score']:.2f} "
                      f"| Predicho: {enriched_event['predicted_score']:.2f} "
                      f"| Error: {enriched_event['prediction_error']:.2f}")
            else:
                # 3. Guardar evento rechazado
                persist_rejected(raw_event, reason)
                print(f"❌ Rechazado: {raw_event.get('event_id', 'Unknown')} - Motivo: {reason}")
                
    except KeyboardInterrupt:
        print("\nDeteniendo el consumidor...")
    finally:
        consumer.close()
        cursor.close()
        conn.close()

if __name__ == "__main__":
    start_consumer()