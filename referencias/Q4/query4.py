#!/usr/bin/env python3
"""
Q4 TPC-H con REFERENCIAS: 30 iteraciones
Order Priority Checking Query
"""
import requests
import time
from pymongo import MongoClient
import threading
import csv
from datetime import datetime, timedelta

MONGOS_URI = "mongodb://10.145.0.173:27017/"
ENDPOINTS = {
    "shard1": "http://10.145.0.173:8080/metrics",
    "shard2": "http://10.145.0.175:8080/metrics",
    "shard3": "http://10.145.0.176:8080/metrics"
}
SAMPLE_INTERVAL = 2
ITERATIONS = 30

# Parámetros de la query
O_ORDERDATE_START = datetime(1993, 7, 1)
O_ORDERDATE_END = O_ORDERDATE_START + timedelta(days=90)  # 3 meses

sampling = False
csv_writer = None
csv_file_handle = None

def get_power(endpoint):
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
    global sampling, csv_writer, csv_file_handle

    while sampling:
        timestamp = time.time()
        elapsed = timestamp - start_time

        p1 = get_power(ENDPOINTS["shard1"])
        p2 = get_power(ENDPOINTS["shard2"])
        p3 = get_power(ENDPOINTS["shard3"])

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
print("🧪 Q4 TPC-H con REFERENCIAS: Order Priority Checking")
print(f"📊 Iteraciones: {ITERATIONS}")
print(f"⏱️  Sampling: cada {SAMPLE_INTERVAL} segundos")
print(f"📋 Parámetros: DATE >= '{O_ORDERDATE_START.date()}' AND < '{O_ORDERDATE_END.date()}'")
print("="*70)

client = MongoClient(MONGOS_URI, serverSelectionTimeoutMS=5000)
db = client.tpch_references

try:
    db.command("ping")
    print("✅ Conectado a MongoDB\n")
except Exception as e:
    print(f"❌ Error: {e}")
    exit(1)

csv_file = "q4_references_energy_metrics.csv"
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

        sampling = True
        start_time = time.time()
        sampler = threading.Thread(
            target=sample,
            args=("Q4_Order_Priority_References", iteration, start_time),
            daemon=True
        )
        sampler.start()

        print("⏱️  Ejecutando query...")
        result_count = 0
        try:
            result = list(db.orders.aggregate([
                # Filtrar por rango de fechas
                {
                    "$match": {
                        "o_orderdate": {
                            "$gte": O_ORDERDATE_START,
                            "$lt": O_ORDERDATE_END
                        }
                    }
                },
                # Lookup a lineitems
                {
                    "$lookup": {
                        "from": "lineitems",
                        "localField": "_id",
                        "foreignField": "l_orderkey",
                        "as": "lineitems_info"
                    }
                },
                # Filtrar lineitems donde l_commitdate < l_receiptdate
                {
                    "$addFields": {
                        "matching_lineitems": {
                            "$filter": {
                                "input": "$lineitems_info",
                                "as": "li",
                                "cond": {
                                    "$lt": ["$$li.l_commitdate", "$$li.l_receiptdate"]
                                }
                            }
                        }
                    }
                },
                # EXISTS: al menos un lineitem que cumpla la condición
                {
                    "$match": {
                        "matching_lineitems": {"$ne": []}
                    }
                },
                # Group by o_orderpriority
                {
                    "$group": {
                        "_id": "$o_orderpriority",
                        "order_count": {"$sum": 1}
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "o_orderpriority": "$_id",
                        "order_count": 1
                    }
                },
                # Sort por o_orderpriority
                {
                    "$sort": {
                        "o_orderpriority": 1
                    }
                }
            ], allowDiskUse=True))
            result_count = len(result)
        except Exception as e:
            print(f"❌ Error en query: {e}")
            result_count = -1

        # Detener sampling INMEDIATAMENTE después de la query
        duration = time.time() - start_time
        sampling = False
        sampler.join(timeout=3)

        if result_count >= 0:
            print(f"✅ Completada en {duration:.3f}s ({duration/3600:.2f} horas)")
            print(f"📊 Resultados: {result_count} categorías de prioridad")
            if result_count > 0:
                print(f"📋 Ejemplo: {result[0]}")
        else:
            print(f"❌ Query falló después de {duration:.3f}s")

        if iteration < ITERATIONS:
            print(f"\n⏳ Esperando 5 segundos antes de siguiente iteración...")
            time.sleep(5)

finally:
    csv_file_handle.close()

print(f"\n{'='*70}")
print(f"✅ COMPLETADO")
print(f"{'='*70}")
print(f"📄 Archivo: {csv_file}")
print(f"📊 Cada fila = 1 sample de energía cada {SAMPLE_INTERVAL}s")
print(f"{'='*70}")

client.close()
