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

def clear_ram_and_restart_mongodb():
    """Limpia caché del SO y reinicia MongoDB para caché frío"""
    # Limpiar RAM del SO
    print("  🧹 Limpiando RAM local (10.145.0.173)...")
    try:
        subprocess.run(['sudo', '/usr/local/bin/clean_ram.sh'],
                      check=True, capture_output=True, timeout=10)
        print("  ✅ RAM del SO limpiada en 10.145.0.173")
    except Exception as e:
        print(f"  ⚠️  Error limpiando RAM local: {e}")

    remote_hosts = ["10.145.0.175", "10.145.0.176"]
    for host in remote_hosts:
        cmd = ['ssh', f'martin@{host}', 'sudo', '/usr/local/bin/clean_ram.sh']
        try:
            subprocess.run(cmd, check=True, capture_output=True, timeout=10)
            print(f"  ✅ RAM del SO limpiada en {host}")
        except Exception as e:
            print(f"  ⚠️  Error limpiando RAM en {host}: {e}")
    
    print("  🔄 Reiniciando MongoDB para limpiar caché interno...")
    
    # Reiniciar servicios correctos en 10.145.0.173
    local_services = ['mongod-shard1', 'mongod-config']
    for service in local_services:
        try:
            subprocess.run(['sudo', 'systemctl', 'restart', service],
                          check=True, capture_output=True, timeout=30)
            print(f"  ✅ {service} reiniciado en 10.145.0.173")
        except Exception as e:
            print(f"  ⚠️  Error reiniciando {service}: {e}")
    
    # Reiniciar mongod en shards remotos
    for host in remote_hosts:
        cmd = ['ssh', f'martin@{host}', 'sudo', 'systemctl', 'restart', 'mongod']
        try:
            subprocess.run(cmd, check=True, capture_output=True, timeout=30)
            print(f"  ✅ MongoDB reiniciado en {host}")
        except Exception as e:
            print(f"  ⚠️  Error reiniciando MongoDB en {host}: {e}")
    
    print("  ⏳ Esperando 20 segundos para que MongoDB arranque...")
    time.sleep(20)
    
    # Verificar MongoDB
    retry_count = 0
    max_retries = 10
    while retry_count < max_retries:
        try:
            test_client = MongoClient(MONGOS_URI, serverSelectionTimeoutMS=2000)
            test_client.admin.command('ping')
            test_client.close()
            print("  ✅ MongoDB listo y accesible")
            break
        except Exception as e:
            retry_count += 1
            if retry_count < max_retries:
                print(f"  ⏳ Esperando MongoDB... intento {retry_count}/{max_retries}")
                time.sleep(3)
            else:
                print(f"  ⚠️  MongoDB no responde después de {max_retries} intentos")
                raise

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
print("🧪 TPC-H Query 8: National Market Share Query")
print("🧊 MODO: Caché frío (reinicia MongoDB cada iteración)")
print(f"📊 Iteraciones: {ITERATIONS}")
print(f"⏱️  Sampling: cada {SAMPLE_INTERVAL} segundos")
print(f"⏰ Timeout por iteración: {QUERY_TIMEOUT/3600:.1f} horas")
print("="*70)

client = MongoClient(MONGOS_URI, serverSelectionTimeoutMS=5000)
db = client.tpch_sin_diseno

try:
    db.command("ping")
    print("✅ Conectado a MongoDB")

    count = db.parts.estimated_document_count()
    print(f"📊 Documentos en parts: ~{count:,}")
    print("")

except Exception as e:
    print(f"❌ Error: {e}")
    exit(1)

# ============================================================
# TPC-H Query 8: National Market Share Query
# ============================================================
TARGET_NATION = "BRAZIL"
REGION_NAME = "AMERICA"
PART_TYPE = "ECONOMY ANODIZED STEEL"
START_DATE = datetime(1995, 1, 1)
END_DATE = datetime(1996, 12, 31, 23, 59, 59, 999000)

pipeline_q8 = [
    {
        "$match": {
            "p_type": PART_TYPE
        }
    },
    {
        "$lookup": {
            "from": "lineitems",
            "localField": "p_partkey",
            "foreignField": "l_partkey",
            "as": "lineitems"
        }
    },
    { "$unwind": "$lineitems" },
    {
        "$lookup": {
            "from": "suppliers",
            "localField": "lineitems.l_suppkey",
            "foreignField": "s_suppkey",
            "as": "supplier"
        }
    },
    { "$unwind": "$supplier" },
    {
        "$lookup": {
            "from": "nations",
            "localField": "supplier.s_nationkey",
            "foreignField": "n_nationkey",
            "as": "supp_nation"
        }
    },
    { "$unwind": "$supp_nation" },
    {
        "$lookup": {
            "from": "orders",
            "localField": "lineitems.l_orderkey",
            "foreignField": "o_orderkey",
            "as": "order"
        }
    },
    { "$unwind": "$order" },
    {
        "$match": {
            "order.o_orderdate": {
                "$gte": START_DATE,
                "$lte": END_DATE
            }
        }
    },
    {
        "$lookup": {
            "from": "customers",
            "localField": "order.o_custkey",
            "foreignField": "c_custkey",
            "as": "customer"
        }
    },
    { "$unwind": "$customer" },
    {
        "$lookup": {
            "from": "nations",
            "localField": "customer.c_nationkey",
            "foreignField": "n_nationkey",
            "as": "cust_nation"
        }
    },
    { "$unwind": "$cust_nation" },
    {
        "$lookup": {
            "from": "regions",
            "localField": "cust_nation.n_regionkey",
            "foreignField": "r_regionkey",
            "as": "region"
        }
    },
    { "$unwind": "$region" },
    {
        "$match": {
            "region.r_name": REGION_NAME
        }
    },
    {
        "$addFields": {
            "o_year": { "$year": "$order.o_orderdate" },
            "volume": {
                "$multiply": [
                    "$lineitems.l_extendedprice",
                    { "$subtract": [1, "$lineitems.l_discount"] }
                ]
            },
            "nation": "$supp_nation.n_name"
        }
    },
    {
        "$group": {
            "_id": "$o_year",
            "total_volume": { "$sum": "$volume" },
            "target_volume": {
                "$sum": {
                    "$cond": [
                        { "$eq": ["$nation", TARGET_NATION] },
                        "$volume",
                        0
                    ]
                }
            }
        }
    },
    {
        "$project": {
            "_id": 0,
            "o_year": "$_id",
            "mkt_share": {
                "$cond": [
                    { "$eq": ["$total_volume", 0] },
                    0,
                    { "$divide": ["$target_volume", "$total_volume"] }
                ]
            }
        }
    },
    {
        "$sort": {
            "o_year": 1
        }
    }
]

# Abrir CSV
csv_file = "q8_energy_metrics.csv"
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
        
        # LIMPIAR CACHÉ ANTES DE CADA ITERACIÓN
        print("🧊 Limpiando cachés para ejecución en frío...")
        clear_ram_and_restart_mongodb()
        
        # Reconectar cliente después del reinicio
        client.close()
        client = MongoClient(MONGOS_URI, serverSelectionTimeoutMS=5000)
        db = client.tpch_sin_diseno

        sampling = True
        start_time = time.time()
        sampler = threading.Thread(
            target=sample,
            args=("Q8_National_Market_Share", iteration, start_time),
            daemon=True
        )
        sampler.start()

        # Configurar timeout de 2 horas
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(QUERY_TIMEOUT)

        query_timed_out = False

        print("⏱️  Ejecutando Query 8...")
        try:
            cursor = db.parts.aggregate(pipeline_q8, allowDiskUse=True)

            # Procesar resultados (solo mostrar en consola)
            rows_count = 0
            for doc in cursor:
                rows_count += 1
                print(f"   📊 Año {doc['o_year']}: "
                      f"Market Share={doc['mkt_share']:.4f} "
                      f"({doc['mkt_share']*100:.2f}%)")
            
            print(f"   ✅ Años encontrados: {rows_count}")

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

        # DETENER SAMPLING INMEDIATAMENTE
        duration = time.time() - start_time
        sampling = False
        sampler.join(timeout=1)
        
        # TOMAR MUESTRA FINAL
        print("  📊 Tomando muestra final...")
        p1 = get_power(ENDPOINTS["shard1"])
        p2 = get_power(ENDPOINTS["shard2"])
        p3 = get_power(ENDPOINTS["shard3"])
        
        csv_writer.writerow({
            "query": "Q8_National_Market_Share",
            "iteration": iteration,
            "elapsed_time_seconds": f"{duration:.3f}",
            "power_shard1_watts": f"{p1/1_000_000:.6f}",
            "power_shard2_watts": f"{p2/1_000_000:.6f}",
            "power_shard3_watts": f"{p3/1_000_000:.6f}",
            "power_total_watts": f"{(p1+p2+p3)/1_000_000:.6f}",
            "timestamp": datetime.now().isoformat()
        })
        csv_file_handle.flush()
        print(f"  ✅ Muestra final: {(p1+p2+p3)/1000:.2f} mW")

        if query_timed_out:
            print(f"⚠️  Iteración {iteration} cancelada por timeout ({duration/3600:.2f}h)")
        else:
            print(f"✅ Completada en {duration:.3f}s ({duration/60:.2f} min)")

        if iteration < ITERATIONS:
            print(f"\n⏳ Esperando 5 segundos antes de siguiente iteración...")
            time.sleep(5)

finally:
    csv_file_handle.close()

print(f"\n{'='*70}")
print(f"✅ COMPLETADO")
print(f"{'='*70}")
print(f"📄 Archivo: {csv_file}")
print(f"{'='*70}")

client.close()
