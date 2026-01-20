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
print("🧪 TPC-H Query 4: Order Priority Checking Query")
print(f"📊 Iteraciones: {ITERATIONS}")
print(f"⏱️  Sampling: cada {SAMPLE_INTERVAL} segundos")
print(f"⏰ Timeout por iteración: {QUERY_TIMEOUT/3600:.1f} horas")
print("="*70)

client = MongoClient(MONGOS_URI, serverSelectionTimeoutMS=5000)
db = client.tpch_sin_diseno

try:
    db.command("ping")
    print("✅ Conectado a MongoDB")

    count = db.orders.estimated_document_count()
    print(f"📊 Documentos en orders: ~{count:,}")
    print("")

except Exception as e:
    print(f"❌ Error: {e}")
    exit(1)

# ============================================================
# TPC-H Query 4: Order Priority Checking Query
# ============================================================
START_DATE = datetime(1993, 7, 1)
END_DATE = datetime(1993, 10, 1)  # 3 meses después

pipeline_q4 = [
    {
        "$match": {
            "o_orderdate": {
                "$gte": START_DATE,
                "$lt": END_DATE
            }
        }
    },
    {
        "$lookup": {
            "from": "lineitems",
            "let": { "orderkey": "$o_orderkey" },
            "pipeline": [
                {
                    "$match": {
                        "$expr": {
                            "$and": [
                                { "$eq": ["$l_orderkey", "$$orderkey"] },
                                { "$lt": ["$l_commitdate", "$l_receiptdate"] }
                            ]
                        }
                    }
                },
                { "$limit": 1 }
            ],
            "as": "matching_lineitems"
        }
    },
    {
        "$match": {
            "matching_lineitems": { "$ne": [] }
        }
    },
    {
        "$group": {
            "_id": "$o_orderpriority",
            "order_count": { "$sum": 1 }
        }
    },
    {
        "$project": {
            "_id": 0,
            "o_orderpriority": "$_id",
            "order_count": 1
        }
    },
    {
        "$sort": {
            "o_orderpriority": 1
        }
    }
]

# Abrir CSV
csv_file = "q4_energy_metrics.csv"
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
            args=("Q4_Order_Priority_Checking", iteration, start_time),
            daemon=True
        )
        sampler.start()

        # Configurar timeout de 2 horas
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(QUERY_TIMEOUT)

        query_timed_out = False

        print("⏱️  Ejecutando Query 4...")
        try:
            cursor = db.orders.aggregate(pipeline_q4, allowDiskUse=True)

            # Procesar resultados
            rows_count = 0
            for doc in cursor:
                rows_count += 1
                print(f"   📋 {doc['o_orderpriority']}: "
                      f"{doc['order_count']} órdenes")

            print(f"   ✅ Prioridades encontradas: {rows_count}")

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
