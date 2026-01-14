#!/usr/bin/env python3
"""
Q19 TPC-H: 30 iteraciones
CADA SAMPLE = UNA FILA EN EL CSV
"""
import requests
import time
from pymongo import MongoClient
import threading
import csv
from datetime import datetime

MONGOS_URI = "mongodb://10.145.0.173:27017/"
ENDPOINTS = {
    "shard1": "http://10.145.0.173:8080/metrics",
    "shard2": "http://10.145.0.175:8080/metrics",
    "shard3": "http://10.145.0.176:8080/metrics"
}
SAMPLE_INTERVAL = 2
ITERATIONS = 30

sampling = False
csv_writer = None
csv_file_handle = None

def get_power(endpoint):
    """
    Obtiene la potencia TOTAL de todos los procesos MongoDB en el nodo
    (incluyendo mongod shards, mongos y config server)
    """
    try:
        resp = requests.get(endpoint, timeout=2)
        power = 0

        for line in resp.text.split('\n'):
            if 'scaph_process_power_consumption_microwatts' in line and \
               ('mongod' in line or 'mongos' in line) and not line.startswith('#'):
                try:
                    power += float(line.split()[-1])
                except (ValueError, IndexError):
                    continue

        return power
    except Exception as e:
        print(f"⚠️  Error obteniendo métricas: {e}")
        return 0

def sample(query_name, iteration, start_time):
    """Toma samples y los escribe DIRECTAMENTE al CSV"""
    global sampling, csv_writer, csv_file_handle

    while sampling:
        timestamp = time.time()
        elapsed = timestamp - start_time

        # Obtener potencia TOTAL de cada nodo (todos los procesos MongoDB)
        p1 = get_power(ENDPOINTS["shard1"])  # nodo 173: shard1 + mongos + config
        p2 = get_power(ENDPOINTS["shard2"])  # nodo 175: solo shard2
        p3 = get_power(ENDPOINTS["shard3"])  # nodo 176: solo shard3

        # Escribir fila INMEDIATAMENTE
        csv_writer.writerow({
            "query": query_name,
            "iteration": iteration,
            "elapsed_time_seconds": f"{elapsed:.3f}",
            "power_shard1_watts": f"{p1/1_000_000:.6f}",
            "power_shard2_watts": f"{p2/1_000_000:.6f}",
            "power_shard3_watts": f"{p3/1_000_000:.6f}",
            "power_total_watts": f"{(p1+p2+p3)/1_000_000:.6f}",
            "timestamp": datetime.now().isoformat()
        })
        csv_file_handle.flush()

        print(f"  📊 Sample en t={elapsed:.1f}s: {(p1+p2+p3)/1000:.2f} mW")
        time.sleep(SAMPLE_INTERVAL)

print("="*70)
print("🧪 Q19 TPC-H: Cada sample = 1 fila en CSV")
print(f"📊 Iteraciones: {ITERATIONS}")
print(f"⏱️  Sampling: cada {SAMPLE_INTERVAL} segundos")
print("="*70)

client = MongoClient(MONGOS_URI, serverSelectionTimeoutMS=5000)
db = client.tpch_optimized

try:
    db.command("ping")
    print("✅ Conectado a MongoDB\n")
except Exception as e:
    print(f"❌ Error: {e}")
    exit(1)

# Abrir CSV UNA SOLA VEZ
csv_file = "q19_energy_metrics.csv"
csv_file_handle = open(csv_file, 'w', newline='')
csv_writer = csv.DictWriter(csv_file_handle, fieldnames=[
    "query", "iteration", "elapsed_time_seconds",
    "power_shard1_watts", "power_shard2_watts", "power_shard3_watts",
    "power_total_watts", "timestamp"
])
csv_writer.writeheader()
csv_file_handle.flush()

try:
    for iteration in range(1, ITERATIONS + 1):
        print(f"\n{'='*70}")
        print(f"🔄 Iteración {iteration}/{ITERATIONS}")
        print(f"{'='*70}")

        # Iniciar sampling
        sampling = True
        start_time = time.time()
        sampler = threading.Thread(
            target=sample,
            args=("Q19_Discounted_Revenue", iteration, start_time),
            daemon=True
        )
        sampler.start()

        # Ejecutar query
        print("⏱️  Ejecutando query...")
        try:
            result = list(db.orders_with_lineitems.aggregate([
                { "$unwind": "$lineitems" },
                {
                    "$lookup": {
                        "from": "parts_with_suppliers",
                        "localField": "lineitems.l_partkey",
                        "foreignField": "p_partkey",
                        "as": "part"
                    }
                },
                { "$unwind": "$part" },
                {
                    "$match": {
                        "$or": [
                            {
                                "part.p_brand": "Brand#12",
                                "part.p_container": { "$in": ["SM CASE", "SM BOX", "SM PACK", "SM PKG"] },
                                "lineitems.l_quantity": { "$gte": 1, "$lte": 11 },
                                "part.p_size": { "$gte": 1, "$lte": 5 },
                                "lineitems.l_shipmode": { "$in": ["AIR", "AIR REG"] },
                                "lineitems.l_shipinstruct": "DELIVER IN PERSON"
                            },
                            {
                                "part.p_brand": "Brand#23",
                                "part.p_container": { "$in": ["MED BAG", "MED BOX", "MED PKG", "MED PACK"] },
                                "lineitems.l_quantity": { "$gte": 10, "$lte": 20 },
                                "part.p_size": { "$gte": 1, "$lte": 10 },
                                "lineitems.l_shipmode": { "$in": ["AIR", "AIR REG"] },
                                "lineitems.l_shipinstruct": "DELIVER IN PERSON"
                            },
                            {
                                "part.p_brand": "Brand#34",
                                "part.p_container": { "$in": ["LG CASE", "LG BOX", "LG PACK", "LG PKG"] },
                                "lineitems.l_quantity": { "$gte": 20, "$lte": 30 },
                                "part.p_size": { "$gte": 1, "$lte": 15 },
                                "lineitems.l_shipmode": { "$in": ["AIR", "AIR REG"] },
                                "lineitems.l_shipinstruct": "DELIVER IN PERSON"
                            }
                        ]
                    }
                },
                {
                    "$group": {
                        "_id": None,
                        "revenue": {
                            "$sum": {
                                "$multiply": [
                                    "$lineitems.l_extendedprice",
                                    { "$subtract": [1, "$lineitems.l_discount"] }
                                ]
                            }
                        }
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "revenue": 1
                    }
                }
            ], allowDiskUse=True))
        except Exception as e:
            print(f"❌ Error: {e}")

        duration = time.time() - start_time

        # Esperar último sample
        time.sleep(SAMPLE_INTERVAL)

        # Detener sampling
        sampling = False
        sampler.join(timeout=3)

        print(f"✅ Completada en {duration:.3f}s")

        if iteration < ITERATIONS:
            print(f"\n⏳ Esperando 3 segundos...")
            time.sleep(3)

finally:
    csv_file_handle.close()

print(f"\n{'='*70}")
print(f"✅ COMPLETADO")
print(f"{'='*70}")
print(f"📄 Archivo: {csv_file}")
print(f"📊 Cada fila = 1 sample de energía cada {SAMPLE_INTERVAL}s")
print(f"{'='*70}")

client.close()
