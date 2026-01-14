#!/usr/bin/env python3
"""
Q3 TPC-H con REFERENCIAS: 30 iteraciones
Shipping Priority Query
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

# Parámetros de la query
C_MKTSEGMENT = "BUILDING"
O_ORDERDATE = datetime(1995, 3, 15)

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
print("🧪 Q3 TPC-H con REFERENCIAS: Shipping Priority")
print(f"📊 Iteraciones: {ITERATIONS}")
print(f"⏱️  Sampling: cada {SAMPLE_INTERVAL} segundos")
print(f"📋 Parámetros: SEGMENT='{C_MKTSEGMENT}', DATE='{O_ORDERDATE.date()}'")
print("="*70)

client = MongoClient(MONGOS_URI, serverSelectionTimeoutMS=5000)
db = client.tpch_references

try:
    db.command("ping")
    print("✅ Conectado a MongoDB\n")
except Exception as e:
    print(f"❌ Error: {e}")
    exit(1)

csv_file = "q3_references_energy_metrics.csv"
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
            args=("Q3_Shipping_Priority_References", iteration, start_time),
            daemon=True
        )
        sampler.start()

        print("⏱️  Ejecutando query...")
        try:
            result = list(db.customers.aggregate([
                {
                    "$match": {
                        "c_mktsegment": C_MKTSEGMENT
                    }
                },
                {
                    "$lookup": {
                        "from": "orders",
                        "localField": "_id",
                        "foreignField": "o_custkey",
                        "as": "orders_info"
                    }
                },
                {"$unwind": "$orders_info"},
                {
                    "$match": {
                        "orders_info.o_orderdate": {"$lt": O_ORDERDATE}
                    }
                },
                {
                    "$lookup": {
                        "from": "lineitems",
                        "localField": "orders_info._id",
                        "foreignField": "l_orderkey",
                        "as": "lineitems_info"
                    }
                },
                {"$unwind": "$lineitems_info"},
                {
                    "$match": {
                        "lineitems_info.l_shipdate": {"$gt": O_ORDERDATE}
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "l_orderkey": "$lineitems_info.l_orderkey",
                            "o_orderdate": "$orders_info.o_orderdate",
                            "o_shippriority": "$orders_info.o_shippriority"
                        },
                        "revenue": {
                            "$sum": {
                                "$multiply": [
                                    "$lineitems_info.l_extendedprice",
                                    {"$subtract": [1, "$lineitems_info.l_discount"]}
                                ]
                            }
                        }
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "l_orderkey": "$_id.l_orderkey",
                        "revenue": 1,
                        "o_orderdate": "$_id.o_orderdate",
                        "o_shippriority": "$_id.o_shippriority"
                    }
                },
                {
                    "$sort": {
                        "revenue": -1,
                        "o_orderdate": 1
                    }
                },
                {"$limit": 10}
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
