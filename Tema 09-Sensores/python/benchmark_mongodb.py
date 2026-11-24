"""
benchmark_mongodb.py  (v2 — corrigido e aprimorado)
====================================================
Correções aplicadas (v2):
  [C2] Isolamento de estado: drop + recriação da coleção TS antes de cada
       nível + warm-up de 3s descartado das métricas.
  [C4] Janela de escrita ampliada para 30s (configurável).
  [C5] Índice explícito removido do schema (Time Series já tem índice
       clusterizado nativo {sensor_id:1, ts:1} no MongoDB 7.0).
  [C6] Auditoria de completude: compara documentos enviados vs persistidos.

Conexão (Windows + Docker Desktop):
  directConnection=true → conecta direto ao PRIMARY, sem resolver
  mongo1/mongo2/mongo3 (nomes só existem na rede Docker interna).

Uso:
    python benchmark_mongodb.py
"""

import random
import statistics
import time
from datetime import datetime, timedelta, timezone
from typing import NamedTuple

from pymongo import MongoClient, DESCENDING
from pymongo.errors import BulkWriteError
from pymongo.write_concern import WriteConcern

# ─── Configurações ──────────────────────────────────────────────────────────
MONGO_URI_BASE        = "mongodb://localhost:27017/?directConnection=true"
DB_NAME               = "fazenda"
COLLECTION_NAME       = "leituras_sensores"
TOTAL_SENSORES        = 500
SENSOR_ALVO           = 42
LEITURAS_POR_SENSOR_HORA = 3_600
REPETICOES_LEITURA    = 100
TAMANHO_LOTE          = 500
DURACAO_ESCRITA       = 30   # segundos de medição efetiva  [C4]
DURACAO_WARMUP        = 3    # segundos de warm-up descartados [C2]


class Resultado(NamedTuple):
    write_concern: str
    throughput_ops_s: float
    lat_media_ms: float
    lat_p95_ms: float
    lat_p99_ms: float
    registros_enviados: int
    registros_confirmados: int


# ─── Conexão ────────────────────────────────────────────────────────────────

def conectar(w=1, j=False) -> tuple[MongoClient, object]:
    """
    directConnection=true: evita resolução de mongo1/mongo2/mongo3.
    WriteConcern análogo aos ConsistencyLevels do Cassandra:
      w=1          → confirmação só no PRIMARY    (~ CL ONE)
      w="majority" → confirmação na maioria       (~ CL QUORUM)
      w=3          → confirmação em todos         (~ CL ALL)
    """
    client = MongoClient(MONGO_URI_BASE, serverSelectionTimeoutMS=10_000)
    db = client[DB_NAME].with_options(write_concern=WriteConcern(w=w, j=j))
    return client, db[COLLECTION_NAME]


# ─── Isolamento: recriar coleção TS entre rodadas ────────────────────────────

def recriar_colecao(client: MongoClient) -> object:
    """
    [C2] Drop + recriação da Time Series Collection.
    Garante estado limpo entre os níveis de WriteConcern.
    Nota: drop() em TS collection também remove system.buckets.* automaticamente.
    """
    db = client[DB_NAME]
    print("    [C2] Recriando coleção para isolar estado...")
    if COLLECTION_NAME in db.list_collection_names():
        db[COLLECTION_NAME].drop()
        time.sleep(1)  # aguarda WiredTiger finalizar checkpoints

    # [C5] NÃO criar índice explícito {sensor_id:1, ts:-1}:
    # MongoDB 7.0 Time Series já cria índice clusterizado nativo
    # {sensor_id:1, ts:1}. Índice extra = overhead de escrita desnecessário.
    db.create_collection(
        COLLECTION_NAME,
        timeseries={
            "timeField":   "ts",
            "metaField":   "sensor_id",
            "granularity": "seconds",
        },
        expireAfterSeconds=604800,
    )
    time.sleep(1)
    print("    [C2] Coleção recriada (sem índice redundante [C5]).")
    return db[COLLECTION_NAME]


# ─── Gerador de documentos ──────────────────────────────────────────────────

def gerar_documento(sensor_id: int, ts: datetime,
                    introduzir_outlier: bool = False) -> dict:
    """
    Gera leitura simulada. Com introduzir_outlier=True simula erros de telemetria
    (outlier de temperatura) conforme taxonomia de Bhadauria et al.
    """
    if introduzir_outlier and random.random() < 0.005:   # 0,5% de outliers
        temperatura = round(random.uniform(100.0, 150.0), 2)  # fisicamente impossível
    else:
        temperatura = round(random.uniform(15.0, 40.0), 2)
    return {
        "sensor_id":   sensor_id,
        "ts":          ts,
        "temperatura": temperatura,
        "umidade":     round(random.uniform(40.0, 95.0), 2),
    }


# ─── Passo 1: Popular dados simulados ───────────────────────────────────────

def popular_dados_hora(col, agora: datetime) -> int:
    print(f"\n[1/3] Populando 1h de dados para sensor {SENSOR_ALVO}...")
    inicio = agora - timedelta(hours=1)
    todos  = [
        gerar_documento(SENSOR_ALVO, inicio + timedelta(seconds=s))
        for s in range(LEITURAS_POR_SENSOR_HORA)
    ]
    t0 = time.perf_counter()
    for i in range(0, len(todos), TAMANHO_LOTE):
        col.insert_many(todos[i: i + TAMANHO_LOTE], ordered=False)
    elapsed = time.perf_counter() - t0
    n = len(todos)
    print(f"    ✓ {n} docs em {elapsed:.2f}s ({n / elapsed:.0f} ops/s)")
    return n


# ─── Passo 2: Benchmark de escrita ──────────────────────────────────────────

def benchmark_escrita(col, agora: datetime) -> tuple[float, int, int]:
    """
    [C2] Warm-up descartado + [C4] janela de DURACAO_ESCRITA segundos.
    [C6] Conta enviados vs confirmados (BulkWriteError parcial capturado).
    """
    duracao_total = DURACAO_WARMUP + DURACAO_ESCRITA
    print(f"\n[2/3] Benchmark de ESCRITA — {TOTAL_SENSORES} sensores | "
          f"warm-up {DURACAO_WARMUP}s + medição {DURACAO_ESCRITA}s...")

    total_enviados    = 0
    total_confirmados = 0
    t_inicio          = time.perf_counter()
    t_medicao_inicio  = None
    tick              = 0

    while True:
        elapsed = time.perf_counter() - t_inicio
        if elapsed >= duracao_total:
            break

        em_warmup = elapsed < DURACAO_WARMUP
        if not em_warmup and t_medicao_inicio is None:
            t_medicao_inicio = time.perf_counter()

        ts_tick = agora + timedelta(seconds=tick)
        lote    = [gerar_documento(sid, ts_tick) for sid in range(TOTAL_SENSORES)]
        enviados_tick = len(lote)

        try:
            result = col.insert_many(lote, ordered=False)
            confirmados_tick = len(result.inserted_ids)
        except BulkWriteError as bwe:
            # Escritas parciais: alguns documentos foram inseridos
            confirmados_tick = bwe.details.get("nInserted", 0)

        if not em_warmup:
            total_enviados    += enviados_tick
            total_confirmados += confirmados_tick

        tick += 1

    duracao_ef = time.perf_counter() - (t_medicao_inicio or t_inicio)
    throughput = total_enviados / duracao_ef if duracao_ef > 0 else 0
    print(f"    ✓ Enviados={total_enviados} | Confirmados={total_confirmados} | "
          f"{duracao_ef:.1f}s | {throughput:.0f} ops/s")
    return throughput, total_enviados, total_confirmados


# ─── Passo 3: Benchmark de leitura ──────────────────────────────────────────

def benchmark_leitura(col, agora: datetime) -> tuple[float, float, float]:
    print(f"\n[3/3] Benchmark de LEITURA — última hora do sensor {SENSOR_ALVO}...")
    inicio_janela = agora - timedelta(hours=1)
    filtro   = {"sensor_id": SENSOR_ALVO, "ts": {"$gte": inicio_janela}}
    projecao = {"_id": 0, "sensor_id": 1, "ts": 1,
                "temperatura": 1, "umidade": 1}

    latencias = []
    for i in range(REPETICOES_LEITURA):
        t0   = time.perf_counter()
        docs = list(col.find(filtro, projecao).sort("ts", DESCENDING).limit(3700))
        lat  = (time.perf_counter() - t0) * 1000
        latencias.append(lat)
        if i == 0:
            print(f"    1ª execução: {len(docs)} docs, latência={lat:.2f}ms")

    latencias.sort()
    media = statistics.mean(latencias)
    p95   = latencias[int(0.95 * len(latencias))]
    p99   = latencias[int(0.99 * len(latencias))]
    print(f"    ✓ Média={media:.2f}ms | P95={p95:.2f}ms | P99={p99:.2f}ms")
    return media, p95, p99


# ─── Passo 4: Loop de WriteConcerns ─────────────────────────────────────────

def testar_write_concerns(agora: datetime) -> list[Resultado]:
    configs = [
        ("w=1 (eventual)",     1,          False),
        ("w=majority (forte)", "majority", True),
        ("w=3 (todos)",        3,          True),
    ]
    resultados = []

    print("\n" + "=" * 65)
    print("TESTE DE WRITE CONCERN — w=1 / w=majority / w=3")
    print(f"Warm-up: {DURACAO_WARMUP}s | Medição: {DURACAO_ESCRITA}s | "
          f"Sem índice redundante [C5]")
    print("=" * 65)

    for rotulo, w, j in configs:
        print(f"\n{'─' * 65}")
        print(f"  WriteConcern: {rotulo}")
        print(f"{'─' * 65}")
        client, col = conectar(w=w, j=j)
        try:
            col_limpa = recriar_colecao(client)   # [C2]
            popular_dados_hora(col_limpa, agora)
            throughput, enviados, confirmados = benchmark_escrita(col_limpa, agora)
            media, p95, p99 = benchmark_leitura(col_limpa, agora)
            resultados.append(Resultado(
                rotulo, throughput, media, p95, p99, enviados, confirmados
            ))
        except Exception as e:
            print(f"    [!] ERRO com {rotulo}: {e}")
            resultados.append(Resultado(rotulo, 0, 0, 0, 0, 0, 0))
        finally:
            client.close()

    return resultados


# ─── Relatório ──────────────────────────────────────────────────────────────

def imprimir_relatorio(resultados: list[Resultado]) -> None:
    print("\n" + "=" * 80)
    print("RELATÓRIO MONGODB v2 — THROUGHPUT / LATÊNCIA / COMPLETUDE")
    print("=" * 80)
    print(f"{'Write Concern':<22} {'Throughput':>14} {'Lat.Média':>12} "
          f"{'P95':>10} {'P99':>10} {'Completude':>12}")
    print(f"{'':22} {'(ops/s)':>14} {'(ms)':>12} "
          f"{'(ms)':>10} {'(ms)':>10} {'(conf/env)':>12}")
    print("-" * 80)
    for r in resultados:
        completude = (f"{r.registros_confirmados}/{r.registros_enviados}"
                      if r.registros_enviados > 0 else "N/A")
        print(f"{r.write_concern:<22} {r.throughput_ops_s:>14,.0f} "
              f"{r.lat_media_ms:>12.2f} {r.lat_p95_ms:>10.2f} "
              f"{r.lat_p99_ms:>10.2f} {completude:>12}")
    print("=" * 80)

    import os
    os.makedirs("../resultados", exist_ok=True)
    with open("../resultados/mongodb_benchmark.csv", "w", encoding="utf-8") as f:
        f.write("write_concern,throughput_ops_s,lat_media_ms,lat_p95_ms,lat_p99_ms,"
                "registros_enviados,registros_confirmados\n")
        for r in resultados:
            f.write(f"{r.write_concern},{r.throughput_ops_s:.2f},{r.lat_media_ms:.2f},"
                    f"{r.lat_p95_ms:.2f},{r.lat_p99_ms:.2f},"
                    f"{r.registros_enviados},{r.registros_confirmados}\n")
    print("[✓] Resultados em resultados/mongodb_benchmark.csv")


if __name__ == "__main__":
    agora = datetime.now(timezone.utc)
    resultados = testar_write_concerns(agora)
    imprimir_relatorio(resultados)
