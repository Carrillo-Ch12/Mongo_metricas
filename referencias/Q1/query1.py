#!/usr/bin/env python3
"""
Q1 TPC-H con REFERENCIAS: 30 iteraciones
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
print("🧪 Q1 TPC-H con REFERENCIAS: Cada sample = 1 fila en CSV")
print(f"📊 Iteraciones: {ITERATIONS}")
print(f"⏱️  Sampling: cada {SAMPLE_INTERVAL} segundos")
print("="*70)

client = MongoClient(MONGOS_URI, serverSelectionTimeoutMS=5000)
db = client.tpch_references

try:
    db.command("ping")
    print("✅ Conectado a MongoDB\n")
except Exception as e:
    print(f"❌ Error: {e}")
    exit(1)

csv_file = "q1_references_energy_metrics.csv"
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
            args=("Q1_Pricing_Summary_References", iteration, start_time),
            daemon=True
        )
        sampler.start()

        print("⏱️  Ejecutando query...")
        try:
            result = list(db.lineitems.aggregate([
                {
                    "$match": {
                        "l_shipdate": {"$lte": datetime(1998, 9, 2)}
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "l_returnflag": "$l_returnflag",
                            "l_linestatus": "$l_linestatus"
                        },
                        "sum_qty": {"$sum": "$l_quantity"},
                        "sum_base_price": {"$sum": "$l_extendedprice"},
                        "sum_disc_price": {
                            "$sum": {
                                "$multiply": ["$l_extendedprice", {"$subtract": [1, "$l_discount"]}]
                            }
                        },
                        "sum_charge": {
                            "$sum": {
                                "$multiply": [
                                    "$l_extendedprice",
                                    {"$subtract": [1, "$l_discount"]},
                                    {"$add": [1, "$l_tax"]}
                                ]
                            }
                        },
                        "avg_qty": {"$avg": "$l_quantity"},
                        "avg_price": {"$avg": "$l_extendedprice"},
                        "avg_disc": {"$avg": "$l_discount"},
                        "count_order": {"$sum": 1}
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "l_returnflag": "$_id.l_returnflag",
                        "l_linestatus": "$_id.l_linestatus",
                        "sum_qty": 1,
                        "sum_base_price": 1,
                        "sum_disc_price": 1,
                        "sum_charge": 1,
                        "avg_qty": 1,
                        "avg_price": 1,
                        "avg_disc": 1,
                        "count_order": 1
                    }
                },
                {
                    "$sort": {
                        "l_returnflag": 1,
                        "l_linestatus": 1
                    }
                }
            ], allowDiskUse=True))
        except Exception as e:
            print(f"❌ Error: {e}")

        duration = time.time() - start_time

        time.sleep(SAMPLE_INTERVAL)

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
