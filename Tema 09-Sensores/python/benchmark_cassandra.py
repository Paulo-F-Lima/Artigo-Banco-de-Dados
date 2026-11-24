"""
benchmark_cassandra_2.py  (v2.1 — corrigido e aprimorado)
=========================================================
Correções aplicadas:
  [C1] Consulta cross-day: usa duas partições (bucket_inicio, bucket_fim)
       quando a janela de leitura atravessa a meia-noite UTC.
  [C2] Isolamento de estado entre níveis: TRUNCATE antes de cada rodada
       + período de warm-up de 3s descartado das métricas.
  [C3] Write-Through completo e auditável: cada amostra de sensor (0..499)
       atualiza simultaneamente `leituras_sensores` e `ultima_leitura` via
       BatchStatement(UNLOGGED) executado em paralelo (execute_concurrent),
       corrigindo a contagem de completude (enviados == confirmados = 500/tick).
  [C4] Janela de escrita efetiva ampliada para 30s (configurável).
  [C6] Auditoria de completude (registros enviados vs. confirmados) e
       resolução automática do diretório `resultados/cassandra_benchmark.csv`.

Uso:
    python benchmark_cassandra_2.py

Pré-requisitos:
    pip install cassandra-driver
"""

import ast
import os
import random
import statistics
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import NamedTuple

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent, execute_concurrent_with_args
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.query import BatchStatement, BatchType, SimpleStatement

# ─── Configurações ──────────────────────────────────────────────────────────
CASSANDRA_HOST           = "127.0.0.1"
KEYSPACE                 = "fazenda"
TOTAL_SENSORES           = 500
SENSOR_ALVO              = 42
LEITURAS_POR_SENSOR_HORA = 3_600
REPETICOES_LEITURA       = 100
CONCURRENCY              = 200
DURACAO_ESCRITA          = 30   # segundos de medição efetiva  [C4]
DURACAO_WARMUP           = 3    # segundos de warm-up descartados [C2]


# ─── Tipos ──────────────────────────────────────────────────────────────────

class Resultado(NamedTuple):
    nivel: str
    throughput_ops_s: float
    lat_media_ms: float
    lat_p95_ms: float
    lat_p99_ms: float
    registros_enviados: int      # amostras de sensores enviadas [C6]
    registros_confirmados: int   # amostras confirmadas (ambas tabelas) [C3][C6]


# ─── Conexão ────────────────────────────────────────────────────────────────

def conectar(consistency: int = ConsistencyLevel.QUORUM):
    cluster = Cluster(
        contact_points=[CASSANDRA_HOST],
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="dc1"),
        protocol_version=4,
    )
    session = cluster.connect(KEYSPACE)
    session.default_consistency_level = consistency
    return cluster, session


# ─── Helpers ────────────────────────────────────────────────────────────────

def bucket_do_dia(ts: datetime) -> str:
    """Retorna a chave de partição diária: 'YYYY-MM-DD'."""
    return ts.strftime("%Y-%m-%d")


def buckets_da_janela(inicio: datetime, fim: datetime) -> list[str]:
    """
    [C1] Retorna lista de buckets únicos que cobrem o intervalo [inicio, fim].
    Normalmente 1 elemento; 2 elementos quando a janela cruza a meia-noite UTC.
    """
    b_inicio = bucket_do_dia(inicio)
    b_fim    = bucket_do_dia(fim)
    if b_inicio == b_fim:
        return [b_fim]
    return [b_inicio, b_fim]


def gerar_leitura(sensor_id: int, ts: datetime,
                  introduzir_outlier: bool = False) -> tuple[int, str, datetime, float, float]:
    """
    Gera tupla (sensor_id, data_bucket, ts, temperatura, umidade).
    Com introduzir_outlier=True, injeta 0,5% de anomalias de domínio
    (temperatura > 100 °C) conforme taxonomia de Bhadauria et al.
    """
    if introduzir_outlier and random.random() < 0.005:
        temperatura = round(random.uniform(100.0, 150.0), 2)
    else:
        temperatura = round(random.uniform(15.0, 40.0), 2)
    umidade = round(random.uniform(40.0, 95.0), 2)
    return (sensor_id, bucket_do_dia(ts), ts, temperatura, umidade)


def resolver_caminho_csv() -> Path:
    """
    Resolve o caminho para `resultados/cassandra_benchmark.csv` tanto se
    executado de dentro de `python/` quanto da raiz do projeto.
    """
    pasta_script = Path(__file__).resolve().parent
    if pasta_script.name == "python":
        pasta_resultados = pasta_script.parent / "resultados"
    else:
        pasta_resultados = pasta_script / "resultados"
    pasta_resultados.mkdir(parents=True, exist_ok=True)
    return pasta_resultados / "cassandra_benchmark.csv"


# ─── Isolamento: truncar tabelas entre rodadas ───────────────────────────────

def truncar_tabelas(session) -> None:
    """
    [C2] Limpa leituras_sensores e ultima_leitura antes de cada nível.
    Garante que métricas de um nível não sejam contaminadas pelo anterior.
    """
    print("    [C2] Truncando tabelas para isolar estado...")
    session.execute("TRUNCATE fazenda.leituras_sensores")
    session.execute("TRUNCATE fazenda.ultima_leitura")
    # Aguarda o Cassandra finalizar flushes/compactions pendentes
    time.sleep(2)
    print("    [C2] Tabelas limpas.")


# ─── Passo 1: Popular dados simulados ───────────────────────────────────────

def popular_dados_hora(session, agora: datetime) -> int:
    """
    Insere 1 hora de histórico (3.600 leituras) para o SENSOR_ALVO
    e sincroniza a tabela auxiliar `ultima_leitura` (Write-Through).
    """
    print(f"\n[1/3] Populando 1h de dados para sensor {SENSOR_ALVO}...")

    prep_leitura = session.prepare("""
        INSERT INTO leituras_sensores
            (sensor_id, data_bucket, ts, temperatura, umidade)
        VALUES (?, ?, ?, ?, ?)
    """)
    prep_ultima = session.prepare("""
        INSERT INTO ultima_leitura
            (sensor_id, ts, temperatura, umidade)
        VALUES (?, ?, ?, ?)
    """)

    inicio_janela = agora - timedelta(hours=1)
    argumentos_leitura = [
        gerar_leitura(SENSOR_ALVO, inicio_janela + timedelta(seconds=segundo))
        for segundo in range(LEITURAS_POR_SENSOR_HORA)
    ]

    t0 = time.perf_counter()
    execute_concurrent_with_args(
        session,
        prep_leitura,
        argumentos_leitura,
        concurrency=CONCURRENCY,
        raise_on_first_error=True,
    )
    # Atualiza ultima_leitura com o registro mais recente do histórico
    ultimo = argumentos_leitura[-1]
    session.execute(prep_ultima, (SENSOR_ALVO, ultimo[2], ultimo[3], ultimo[4]))

    elapsed = time.perf_counter() - t0
    n = len(argumentos_leitura)
    print(f"    ✓ {n} registros em {elapsed:.2f}s ({n / elapsed:.0f} ops/s)")
    return n


# ─── Passo 2: Benchmark de escrita (Write-Through completo) ─────────────────

def benchmark_escrita(session, agora: datetime) -> tuple[float, int, int, datetime]:
    """
    [C3] Write-Through completo: para cada um dos 500 sensores por tick,
         executa um BatchStatement(UNLOGGED) contendo o INSERT em
         `leituras_sensores` e o UPSERT em `ultima_leitura`.
    [C2] Warm-up de DURACAO_WARMUP segundos descartado das métricas.
    [C4] Janela de DURACAO_ESCRITA segundos de medição efetiva.
    [C6] Contabiliza amostras enviadas vs. confirmadas (500 por tick).

    Retorna (throughput_ops_s, enviados, confirmados, ultimo_ts_escrita).
    """
    duracao_total = DURACAO_WARMUP + DURACAO_ESCRITA
    print(f"\n[2/3] Benchmark de ESCRITA (Write-Through) — {TOTAL_SENSORES} sensores | "
          f"warm-up {DURACAO_WARMUP}s + medição {DURACAO_ESCRITA}s...")

    prep_leitura = session.prepare("""
        INSERT INTO leituras_sensores
            (sensor_id, data_bucket, ts, temperatura, umidade)
        VALUES (?, ?, ?, ?, ?)
    """)
    prep_ultima = session.prepare("""
        INSERT INTO ultima_leitura
            (sensor_id, ts, temperatura, umidade)
        VALUES (?, ?, ?, ?)
    """)

    total_enviados    = 0
    total_confirmados = 0
    t_inicio          = time.perf_counter()
    t_medicao_inicio  = None
    tick              = 0
    ultimo_ts         = agora

    while True:
        elapsed = time.perf_counter() - t_inicio
        if elapsed >= duracao_total:
            break

        em_warmup = elapsed < DURACAO_WARMUP
        if not em_warmup and t_medicao_inicio is None:
            t_medicao_inicio = time.perf_counter()

        ts_tick   = agora + timedelta(seconds=tick)
        ultimo_ts = ts_tick

        # [C3] Monta 1 batch não-logado por sensor (leituras_sensores + ultima_leitura)
        statements_and_params = []
        for sid in range(TOTAL_SENSORES):
            _, bucket, ts, temp, umid = gerar_leitura(sid, ts_tick)
            batch = BatchStatement(
                batch_type=BatchType.UNLOGGED,
                consistency_level=session.default_consistency_level,
            )
            batch.add(prep_leitura, (sid, bucket, ts, temp, umid))
            batch.add(prep_ultima,  (sid, ts, temp, umid))
            statements_and_params.append((batch, None))

        resultados_exec = execute_concurrent(
            session,
            statements_and_params,
            concurrency=CONCURRENCY,
            raise_on_first_error=False,
        )
        confirmados_tick = sum(1 for ok, _ in resultados_exec if ok)

        if not em_warmup:
            total_enviados    += TOTAL_SENSORES
            total_confirmados += confirmados_tick

        tick += 1

    duracao_efetiva = time.perf_counter() - (t_medicao_inicio or t_inicio)
    throughput = total_enviados / duracao_efetiva if duracao_efetiva > 0 else 0.0
    mutacoes_cql = total_confirmados * 2  # leituras_sensores + ultima_leitura

    print(f"    ✓ Amostras: Enviadas={total_enviados} | Confirmadas={total_confirmados} "
          f"(Mutações CQL={mutacoes_cql}) | {duracao_efetiva:.1f}s | "
          f"Throughput={throughput:.0f} ops/s")
    return throughput, total_enviados, total_confirmados, ultimo_ts


# ─── Passo 3: Benchmark de leitura (cross-day safe) ─────────────────────────

def benchmark_leitura(session, agora: datetime,
                      fim_janela: datetime | None = None) -> tuple[float, float, float]:
    """
    [C1] Consulta cobrindo todas as partições diárias (`data_bucket`) entre
         `agora - 1h` e `fim_janela`, evitando perda de registros caso a
         janela atravesse a meia-noite UTC.
    """
    print(f"\n[3/3] Benchmark de LEITURA — última hora do sensor {SENSOR_ALVO}...")

    inicio_janela = agora - timedelta(hours=1)
    limite_superior = fim_janela if fim_janela and fim_janela > agora else agora
    buckets = buckets_da_janela(inicio_janela, limite_superior)

    if len(buckets) == 1:
        stmt = SimpleStatement("""
            SELECT sensor_id, ts, temperatura, umidade
            FROM leituras_sensores
            WHERE sensor_id = %s
              AND data_bucket = %s
              AND ts >= %s
            LIMIT 3700
        """, consistency_level=session.default_consistency_level)

        def executar(s):
            return list(s.execute(stmt, (SENSOR_ALVO, buckets[0], inicio_janela)))
    else:
        # [C1] Janela cruzou a meia-noite UTC: consulta ambas as partições
        stmt_dia1 = SimpleStatement("""
            SELECT sensor_id, ts, temperatura, umidade
            FROM leituras_sensores
            WHERE sensor_id = %s
              AND data_bucket = %s
              AND ts >= %s
            LIMIT 3700
        """, consistency_level=session.default_consistency_level)
        stmt_dia2 = SimpleStatement("""
            SELECT sensor_id, ts, temperatura, umidade
            FROM leituras_sensores
            WHERE sensor_id = %s
              AND data_bucket = %s
            LIMIT 3700
        """, consistency_level=session.default_consistency_level)

        def executar(s):
            r1 = list(s.execute(stmt_dia1, (SENSOR_ALVO, buckets[0], inicio_janela)))
            r2 = list(s.execute(stmt_dia2, (SENSOR_ALVO, buckets[1])))
            return (r1 + r2)[:3700]

        print(f"    [C1] Janela cross-day detectada: buckets={buckets}")

    latencias = []
    for i in range(REPETICOES_LEITURA):
        t0   = time.perf_counter()
        rows = executar(session)
        lat  = (time.perf_counter() - t0) * 1000
        latencias.append(lat)
        if i == 0:
            print(f"    1ª execução: {len(rows)} linhas, latência={lat:.2f}ms")

    latencias.sort()
    media = statistics.mean(latencias)
    p95   = latencias[int(0.95 * len(latencias))]
    p99   = latencias[int(0.99 * len(latencias))]
    print(f"    ✓ Média={media:.2f}ms | P95={p95:.2f}ms | P99={p99:.2f}ms")
    return media, p95, p99


# ─── Passo 4: Loop de níveis de consistência ────────────────────────────────

def testar_consistencia(agora: datetime) -> list[Resultado]:
    niveis = [
        ("ONE",    ConsistencyLevel.ONE),
        ("QUORUM", ConsistencyLevel.QUORUM),
        ("ALL",    ConsistencyLevel.ALL),
    ]
    resultados = []

    print("\n" + "=" * 68)
    print("TESTE DE CONSISTÊNCIA — ONE / QUORUM / ALL")
    print(f"Warm-up: {DURACAO_WARMUP}s | Medição: {DURACAO_ESCRITA}s | "
          f"Write-Through completo [C3]")
    print("=" * 68)

    for nome, cl in niveis:
        print(f"\n{'─' * 68}")
        print(f"  ConsistencyLevel: {nome}")
        print(f"{'─' * 68}")
        cluster, session = conectar(cl)
        try:
            truncar_tabelas(session)        # [C2] isolar estado
            popular_dados_hora(session, agora)
            throughput, enviados, confirmados, ultimo_ts = benchmark_escrita(session, agora)
            media, p95, p99 = benchmark_leitura(session, agora, ultimo_ts)
            resultados.append(Resultado(
                nome, throughput, media, p95, p99, enviados, confirmados
            ))
        except Exception as e:
            print(f"    [!] ERRO com CL={nome}: {e}")
            resultados.append(Resultado(nome, 0.0, 0.0, 0.0, 0.0, 0, 0))
        finally:
            cluster.shutdown()

    return resultados


# ─── Relatório e CSV ─────────────────────────────────────────────────────────

def imprimir_relatorio(resultados: list[Resultado]) -> None:
    print("\n" + "=" * 82)
    print("RELATÓRIO CASSANDRA v2.1 — THROUGHPUT / LATÊNCIA / COMPLETUDE")
    print("=" * 82)
    print(f"{'Nível':<10} {'Throughput':>15} {'Lat.Média':>12} "
          f"{'P95':>10} {'P99':>10} {'Completude':>15}")
    print(f"{'':10} {'(ops/s)':>15} {'(ms)':>12} "
          f"{'(ms)':>10} {'(ms)':>10} {'(conf/env)':>15}")
    print("-" * 82)
    for r in resultados:
        completude = (f"{r.registros_confirmados}/{r.registros_enviados}"
                      if r.registros_enviados > 0 else "N/A")
        print(f"{r.nivel:<10} {r.throughput_ops_s:>15,.0f} {r.lat_media_ms:>12.2f} "
              f"{r.lat_p95_ms:>10.2f} {r.lat_p99_ms:>10.2f} {completude:>15}")
    print("=" * 82)

    caminho_csv = resolver_caminho_csv()
    with open(caminho_csv, "w", encoding="utf-8") as f:
        f.write("nivel,throughput_ops_s,lat_media_ms,lat_p95_ms,lat_p99_ms,"
                "registros_enviados,registros_confirmados\n")
        for r in resultados:
            f.write(f"{r.nivel},{r.throughput_ops_s:.2f},{r.lat_media_ms:.2f},"
                    f"{r.lat_p95_ms:.2f},{r.lat_p99_ms:.2f},"
                    f"{r.registros_enviados},{r.registros_confirmados}\n")
    print(f"[✓] Resultados salvos em: {caminho_csv}")


if __name__ == "__main__":
    agora = datetime.now(timezone.utc)
    print(f"Horário UTC: {agora.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Buckets na janela de 1h: {buckets_da_janela(agora - timedelta(hours=1), agora)}")
    resultados = testar_consistencia(agora)
    imprimir_relatorio(resultados)