#!/usr/bin/env python3
"""
Q2 TPC-H con REFERENCIAS: 30 iteraciones
Minimum Cost Supplier Query
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

# Parámetros de la query (ajusta según TPC-H spec)
P_SIZE = 15
P_TYPE_SUFFIX = "BRASS"  # Para p_type like '%BRASS'
R_NAME = "EUROPE"

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
print("🧪 Q2 TPC-H con REFERENCIAS: Minimum Cost Supplier")
print(f"📊 Iteraciones: {ITERATIONS}")
print(f"⏱️  Sampling: cada {SAMPLE_INTERVAL} segundos")
print(f"📋 Parámetros: SIZE={P_SIZE}, TYPE='%{P_TYPE_SUFFIX}', REGION='{R_NAME}'")
print("="*70)

client = MongoClient(MONGOS_URI, serverSelectionTimeoutMS=5000)
db = client.tpch_references

try:
    db.command("ping")
    print("✅ Conectado a MongoDB\n")
except Exception as e:
    print(f"❌ Error: {e}")
    exit(1)

csv_file = "q2_references_energy_metrics.csv"
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
            args=("Q2_Minimum_Cost_Supplier_References", iteration, start_time),
            daemon=True
        )
        sampler.start()

        print("⏱️  Ejecutando query...")
        try:
            result = list(db.parts.aggregate([
                {
                    "$match": {
                        "p_size": P_SIZE,
                        "p_type": {"$regex": f"{P_TYPE_SUFFIX}$"}
                    }
                },
                {
                    "$lookup": {
                        "from": "partsupp",
                        "localField": "_id",
                        "foreignField": "ps_partkey",
                        "as": "partsupp_info"
                    }
                },
                {"$unwind": "$partsupp_info"},
                {
                    "$lookup": {
                        "from": "suppliers",
                        "localField": "partsupp_info.ps_suppkey",
                        "foreignField": "_id",
                        "as": "supplier_info"
                    }
                },
                {"$unwind": "$supplier_info"},
                {
                    "$lookup": {
                        "from": "nations",
                        "localField": "supplier_info.s_nationkey",
                        "foreignField": "_id",
                        "as": "nation_info"
                    }
                },
                {"$unwind": "$nation_info"},
                {
                    "$lookup": {
                        "from": "regions",
                        "localField": "nation_info.n_regionkey",
                        "foreignField": "_id",
                        "as": "region_info"
                    }
                },
                {"$unwind": "$region_info"},
                {
                    "$match": {
                        "region_info.r_name": R_NAME
                    }
                },
                {
                    "$group": {
                        "_id": "$_id",
                        "p_partkey": {"$first": "$_id"},
                        "p_mfgr": {"$first": "$p_mfgr"},
                        "min_supplycost": {"$min": "$partsupp_info.ps_supplycost"},
                        "suppliers": {"$push": {
                            "ps_supplycost": "$partsupp_info.ps_supplycost",
                            "s_acctbal": "$supplier_info.s_acctbal",
                            "s_name": "$supplier_info.s_name",
                            "s_address": "$supplier_info.s_address",
                            "s_phone": "$supplier_info.s_phone",
                            "s_comment": "$supplier_info.s_comment",
                            "n_name": "$nation_info.n_name"
                        }}
                    }
                },
                {"$unwind": "$suppliers"},
                {
                    "$match": {
                        "$expr": {"$eq": ["$suppliers.ps_supplycost", "$min_supplycost"]}
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "s_acctbal": "$suppliers.s_acctbal",
                        "s_name": "$suppliers.s_name",
                        "n_name": "$suppliers.n_name",
                        "p_partkey": 1,
                        "p_mfgr": 1,
                        "s_address": "$suppliers.s_address",
                        "s_phone": "$suppliers.s_phone",
                        "s_comment": "$suppliers.s_comment"
                    }
                },
                {
                    "$sort": {
                        "s_acctbal": -1,
                        "n_name": 1,
                        "s_name": 1,
                        "p_partkey": 1
                    }
                },
                {"$limit": 100}
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
