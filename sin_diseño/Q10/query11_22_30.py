#!/usr/bin/env python3
"""
Q10 TPC-H – Returned Item Reporting
REANUDACIÓN: iteraciones 22 a 30
CADA SAMPLE = UNA FILA EN EL CSV
"""

import requests
import time
from pymongo import MongoClient
import threading
import csv
from datetime import datetime

# ===================== CONFIG =====================
MONGOS_URI = "mongodb://10.145.0.173:27017/"
DB_NAME = "tpch_optimized"

ENDPOINTS = {
    "shard1": "http://10.145.0.173:8080/metrics",
    "shard2": "http://10.145.0.175:8080/metrics",
    "shard3": "http://10.145.0.176:8080/metrics"
}

SAMPLE_INTERVAL = 2
START_ITERATION = 22
END_ITERATION = 30

CSV_FILE = "q10_energy_metrics_part2.csv"
QUERY_NAME = "Q10_Returned_Items"

# ===================== GLOBALS =====================
sampling = False
csv_writer = None
csv_file_handle = None

# ===================== POWER =====================
def get_power(endpoint):
    """
    Obtiene la potencia TOTAL (W) de todos los procesos MongoDB del nodo
    """
    try:
        resp = requests.get(endpoint, timeout=2)
        power = 0

        for line in resp.text.split("\n"):
            if (
                "scaph_process_power_consumption_microwatts" in line
                and ("mongod" in line or "mongos" in line)
                and not line.startswith("#")
            ):
                try:
                    power += float(line.split()[-1])
                except (ValueError, IndexError):
                    continue

        return power
    except Exception as e:
        print(f"⚠️  Error métricas {endpoint}: {e}")
        return 0

# ===================== SAMPLER =====================
def sample(query_name, iteration, start_time):
    global sampling, csv_writer, csv_file_handle

    while sampling:
        now = time.time()
        elapsed = now - start_time

        p1 = get_power(ENDPOINTS["shard1"])
        p2 = get_power(ENDPOINTS["shard2"])
        p3 = get_power(ENDPOINTS["shard3"])

        csv_writer.writerow({
            "query": query_name,
            "iteration": iteration,
            "elapsed_time_seconds": f"{elapsed:.3f}",
            "power_shard1_watts": f"{p1 / 1_000_000:.6f}",
            "power_shard2_watts": f"{p2 / 1_000_000:.6f}",
            "power_shard3_watts": f"{p3 / 1_000_000:.6f}",
            "power_total_watts": f"{(p1 + p2 + p3) / 1_000_000:.6f}",
            "timestamp": datetime.now().isoformat()
        })
        csv_file_handle.flush()

        print(
            f"  📊 Iter {iteration} | t={elapsed:.1f}s | "
            f"P={(p1+p2+p3)/1_000_000:.3f} W"
        )
        time.sleep(SAMPLE_INTERVAL)

# ===================== MAIN =====================
print("=" * 70)
print("🧪 Q10 TPC-H – REANUDACIÓN")
print(f"🔁 Iteraciones: {START_ITERATION} → {END_ITERATION}")
print(f"⏱️  Sampling: cada {SAMPLE_INTERVAL}s")
print("=" * 70)

client = MongoClient(MONGOS_URI, serverSelectionTimeoutMS=5000)
db = client[DB_NAME]

try:
    db.command("ping")
    print("✅ Conectado a MongoDB\n")
except Exception as e:
    print(f"❌ Error MongoDB: {e}")
    exit(1)

# CSV
csv_file_handle = open(CSV_FILE, "w", newline="")
csv_writer = csv.DictWriter(csv_file_handle, fieldnames=[
    "query",
    "iteration",
    "elapsed_time_seconds",
    "power_shard1_watts",
    "power_shard2_watts",
    "power_shard3_watts",
    "power_total_watts",
    "timestamp"
])
csv_writer.writeheader()
csv_file_handle.flush()

try:
    for iteration in range(START_ITERATION, END_ITERATION + 1):
        print(f"\n{'=' * 70}")
        print(f"🔄 Iteración {iteration}/{END_ITERATION}")
        print(f"{'=' * 70}")

        sampling = True
        start_time = time.time()

        sampler = threading.Thread(
            target=sample,
            args=(QUERY_NAME, iteration, start_time),
            daemon=True
        )
        sampler.start()

        print("⏱️  Ejecutando Q10...")
        try:
            list(db.orders_with_lineitems.aggregate([
                {
                    "$match": {
                        "o_orderdate": {
                            "$gte": "1993-10-01",
                            "$lt": "1994-01-01"
                        }
                    }
                },
                { "$unwind": "$lineitems" },
                { "$match": { "lineitems.l_returnflag": "R" } },
                {
                    "$lookup": {
                        "from": "customers",
                        "localField": "o_custkey",
                        "foreignField": "c_custkey",
                        "as": "customer"
                    }
                },
                { "$unwind": "$customer" },
                {
                    "$project": {
                        "c_custkey": "$customer.c_custkey",
                        "c_name": "$customer.c_name",
                        "c_acctbal": "$customer.c_acctbal",
                        "c_phone": "$customer.c_phone",
                        "n_name": "$customer.c_nation_name",
                        "c_address": "$customer.c_address",
                        "c_comment": "$customer.c_comment",
                        "revenue": {
                            "$multiply": [
                                "$lineitems.l_extendedprice",
                                { "$subtract": [1, "$lineitems.l_discount"] }
                            ]
                        }
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "c_custkey": "$c_custkey",
                            "c_name": "$c_name",
                            "c_acctbal": "$c_acctbal",
                            "c_phone": "$c_phone",
                            "n_name": "$n_name",
                            "c_address": "$c_address",
                            "c_comment": "$c_comment"
                        },
                        "revenue": { "$sum": "$revenue" }
                    }
                },
                { "$sort": { "revenue": -1 } },
                { "$limit": 20 },
                {
                    "$project": {
                        "_id": 0,
                        "c_custkey": "$_id.c_custkey",
                        "c_name": "$_id.c_name",
                        "revenue": 1,
                        "c_acctbal": "$_id.c_acctbal",
                        "n_name": "$_id.n_name",
                        "c_address": "$_id.c_address",
                        "c_phone": "$_id.c_phone",
                        "c_comment": "$_id.c_comment"
                    }
                }
            ], allowDiskUse=True))
        except Exception as e:
            print(f"❌ Error Q10: {e}")

        duration = time.time() - start_time

        time.sleep(SAMPLE_INTERVAL)
        sampling = False
        sampler.join(timeout=3)

        print(f"✅ Iteración {iteration} completada en {duration:.3f}s")

        if iteration < END_ITERATION:
            time.sleep(3)

finally:
    csv_file_handle.close()
    client.close()

print("\n" + "=" * 70)
print("✅ Q10 REANUDACIÓN COMPLETADA")
print(f"📄 CSV: {CSV_FILE}")
print("=" * 70)
