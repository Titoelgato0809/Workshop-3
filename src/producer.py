import csv
import json
import time
from datetime import datetime
from pathlib import Path
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions

# ============================================================
# CONFIGURACIÓN
# ============================================================
BASE_DIR = Path(__file__).resolve().parent.parent
CSV_PATH = BASE_DIR / "data" / "processed" / "happiness_master_processed.csv"
TOPIC = "happiness-events"
BOOTSTRAP_SERVERS = "localhost:9092"

def crear_topico_si_no_existe():
    """Cumple el requerimiento: 3 partitions, replication factor 1 de forma automática"""
    admin_client = AdminClient({'bootstrap.servers': BOOTSTRAP_SERVERS})
    
    # 1. Revisar los tópicos actuales en el cluster de Kafka
    metadata = admin_client.list_topics(timeout=10)
    
    if TOPIC not in metadata.topics:
        # Si no existe en absoluto, lo crea desde cero con 3 particiones
        topico = NewTopic(TOPIC, num_partitions=3, replication_factor=1)
        admin_client.create_topics([topico])
        print(f"Tópico '{TOPIC}' creado desde cero con 3 particiones.")
    else:
        # Si ya existe, contamos cuántas particiones tiene
        particiones_actuales = len(metadata.topics[TOPIC].partitions)
        
        if particiones_actuales < 3:
            print(f"El tópico existe con {particiones_actuales} partición(es). Forzando actualización a 3...")
            # Le indicamos a Kafka que el nuevo total debe ser 3
            nuevas_particiones = [NewPartitions(TOPIC, new_total_count=3)]
            admin_client.create_partitions(nuevas_particiones)
            print(f"Tópico '{TOPIC}' actualizado exitosamente a 3 particiones.")
        else:
            print(f"El tópico '{TOPIC}' ya cumple con tener {particiones_actuales} particiones.")

def convert_if_numeric(value):
    """Convierte strings numéricos manteniendo nulos."""
    if value is None:
        return None
    value = value.strip()
    if value == "":
        return ""
    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        return value

def iniciar_productor():
    crear_topico_si_no_existe()
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    with open(CSV_PATH, encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)

        # enumerate(reader, 1) nos da un contador automático para el event_id
        for idx, row in enumerate(reader, start=1):
            
            # Construcción estricta del JSON según el Event Contract
            event = {
                "event_id": f"E{idx:03d}",
                "country": row["country"].strip().capitalize(),
                "year": convert_if_numeric(row["year"]),
                "gdp": convert_if_numeric(row["gdp_per_capita"]),
                "family": convert_if_numeric(row["social_support"]),
                "health": convert_if_numeric(row["health_life_expectancy"]),
                "freedom": convert_if_numeric(row["freedom"]),
                "generosity": convert_if_numeric(row["generosity"]),
                "corruption": convert_if_numeric(row["trust_corruption"]),
                "actual_happiness_score": convert_if_numeric(row["happiness_score"]),
                "event_time": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S") # ISO-8601
            }

            key = str(event["country"])

            producer.produce(
                topic=TOPIC,
                key=key,
                value=json.dumps(event)
            )
            producer.poll(0)

            print(f"Sent [{event['event_id']}]: key={key} | score={event['actual_happiness_score']} | time={event['event_time']}")
            
            # Retraso configurable requerido por el taller
            time.sleep(0.75)

    producer.flush()
    print("Todos los eventos han sido transmitidos exitosamente.")

if __name__ == "__main__":
    iniciar_productor()