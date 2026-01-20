#!/usr/bin/env python3
import requests
import time
from pymongo import MongoClient
import threading
import csv
import subprocess
from datetime import datetime
import signal

MONGOS_URI = "mongodb://10.145.0.173:27017/"
ENDPOINTS = {
    "shard1": "http://10.145.0.173:8080/metrics",
    "shard2": "http://10.145.0.175:8080/metrics",
    "shard3": "http://10.145.0.176:8080/metrics"
}
SAMPLE_INTERVAL = 2
ITERATIONS = 30
QUERY_TIMEOUT = 7200  # 2 horas = 7200 segundos

sampling = False
csv_writer = None
csv_file_handle = None
query_timed_out = False

class TimeoutException(Exception):
    pass

def timeout_handler(signum, frame):
    raise TimeoutException("Query excedió el tiempo límite de 2 horas")

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

def clear_ram_remote():
    # Limpiar RAM localmente en 10.145.0.173
    print("  🧹 Limpiando RAM local (10.145.0.173)...")
    try:
        subprocess.run(['sudo', '/usr/local/bin/clean_ram.sh'],
                      check=True, capture_output=True, timeout=10)
        print("  ✅ RAM limpiada en 10.145.0.173 (local)")
    except Exception as e:
        print(f"  ⚠️  Error limpiando RAM local: {e}")

    # Limpiar RAM remotamente en shards 2 y 3
    remote_hosts = ["10.145.0.175", "10.145.0.176"]
    for host in remote_hosts:
        cmd = ['ssh', f'martin@{host}', 'sudo', '/usr/local/bin/clean_ram.sh']
        try:
            subprocess.run(cmd, check=True, capture_output=True, timeout=10)
            print(f"  ✅ RAM limpiada en {host}")
        except Exception as e:
            print(f"  ⚠️  Error limpiando RAM en {host}: {e}")

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
print("🧪 TPC-H Query 3: Shipping Priority Query")
print(f"📊 Iteraciones: {ITERATIONS}")
print(f"⏱️  Sampling: cada {SAMPLE_INTERVAL} segundos")
print(f"⏰ Timeout por iteración: {QUERY_TIMEOUT/3600:.1f} horas")
print("="*70)

client = MongoClient(MONGOS_URI, serverSelectionTimeoutMS=5000)
db = client.tpch_sin_diseno

try:
    db.command("ping")
    print("✅ Conectado a MongoDB")

    count = db.customers.estimated_document_count()
    print(f"📊 Documentos en customers: ~{count:,}")
    print("")

except Exception as e:
    print(f"❌ Error: {e}")
    exit(1)

# ============================================================
# TPC-H Query 3: Shipping Priority Query
# ============================================================
C_MKTSEGMENT = "BUILDING"
ORDER_DATE_LIMIT = datetime(1995, 3, 15)
SHIP_DATE_LIMIT = datetime(1995, 3, 15)

pipeline_q3 = [
    {
        "$match": {
            "c_mktsegment": C_MKTSEGMENT
        }
    },
    {
        "$lookup": {
            "from": "orders",
            "localField": "c_custkey",
            "foreignField": "o_custkey",
            "as": "orders"
        }
    },
    { "$unwind": "$orders" },
    {
        "$match": {
            "orders.o_orderdate": { "$lt": ORDER_DATE_LIMIT }
        }
    },
    {
        "$lookup": {
            "from": "lineitems",
            "localField": "orders.o_orderkey",
            "foreignField": "l_orderkey",
            "as": "lineitems"
        }
    },
    { "$unwind": "$lineitems" },
    {
        "$match": {
            "lineitems.l_shipdate": { "$gt": SHIP_DATE_LIMIT }
        }
    },
    {
        "$addFields": {
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
                "l_orderkey": "$orders.o_orderkey",
                "o_orderdate": "$orders.o_orderdate",
                "o_shippriority": "$orders.o_shippriority"
            },
            "revenue": { "$sum": "$revenue" }
        }
    },
    {
        "$project": {
            "_id": 0,
            "l_orderkey": "$_id.l_orderkey",
            "revenue": { "$round": ["$revenue", 2] },
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
    { "$limit": 10 }
]

# Abrir CSV
csv_file = "q3_energy_metrics.csv"
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
            args=("Q3_Shipping_Priority", iteration, start_time),
            daemon=True
        )
        sampler.start()

        # Configurar timeout de 2 horas
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(QUERY_TIMEOUT)

        query_timed_out = False

        print("⏱️  Ejecutando Query 3...")
        try:
            cursor = db.customers.aggregate(pipeline_q3, allowDiskUse=True)

            # Procesar resultados
            rows_count = 0
            for doc in cursor:
                rows_count += 1
                print(f"   📋 Order {doc['l_orderkey']}: "
                      f"Revenue=${doc['revenue']:.2f}, "
                      f"Priority={doc['o_shippriority']}")

            print(f"   ✅ Top órdenes encontradas: {rows_count}")

            # Cancelar alarma si termina antes
            signal.alarm(0)

        except TimeoutException:
            print(f"⏰ TIMEOUT: Query excedió 2 horas en iteración {iteration}")
            query_timed_out = True
            signal.alarm(0)

        except Exception as e:
            print(f"❌ Error en query: {e}")
            import traceback
            traceback.print_exc()
            signal.alarm(0)

        duration = time.time() - start_time
        time.sleep(SAMPLE_INTERVAL)

        sampling = False
        sampler.join(timeout=3)

        if query_timed_out:
            print(f"⚠️  Iteración {iteration} cancelada por timeout ({duration/3600:.2f}h)")
        else:
            print(f"✅ Completada en {duration:.3f}s ({duration/60:.2f} min)")

        # Limpieza de RAM
        clear_ram_remote()

        if iteration < ITERATIONS:
            print(f"\n⏳ Esperando 3 segundos...")
            time.sleep(3)

finally:
    csv_file_handle.close()

print(f"\n{'='*70}")
print(f"✅ COMPLETADO")
print(f"{'='*70}")
print(f"📄 Archivo: {csv_file}")
print(f"{'='*70}")

client.close()
