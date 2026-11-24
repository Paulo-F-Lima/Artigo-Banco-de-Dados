"""
comparar_resultados.py  (v2 — auditoria de completude e idempotência)
======================================================================
Correções v2:
  [C6] Verifica completude: registros_enviados vs registros_confirmados
  [C6] Detecta perda silenciosa (gap) e alerta para divergências > 0,1%
  [C6] Calcula taxa de perda por nível/write-concern
  Comparação direta QUORUM vs w=majority enriquecida

Uso:
    python comparar_resultados.py
"""

import csv
import os
from typing import Optional


# ─── Leitura de CSV ─────────────────────────────────────────────────────────

def ler_csv(caminho: str) -> list[dict]:
    if not os.path.exists(caminho):
        print(f"[!] Arquivo não encontrado: {caminho}")
        return []
    with open(caminho, encoding="utf-8") as f:
        return list(csv.DictReader(f))


# ─── Impressão de tabela ────────────────────────────────────────────────────

def imprimir_tabela(titulo: str, dados: list[dict], col_nivel: str) -> None:
    if not dados:
        return

    # Detecta se há colunas de completude (v2)
    tem_completude = "registros_enviados" in (dados[0].keys() if dados else {})

    print(f"\n{'=' * 78}")
    print(f"  {titulo}")
    print(f"{'=' * 78}")

    if tem_completude:
        print(f"{'Nível/Concern':<22} {'Throughput':>13} {'Lat.Média':>11} "
              f"{'P95':>8} {'P99':>8} {'Completude':>14} {'Perda':>8}")
        print(f"{'':22} {'(ops/s)':>13} {'(ms)':>11} "
              f"{'(ms)':>8} {'(ms)':>8} {'(conf/env)':>14} {'(%)':>8}")
        print("-" * 78)
        for row in dados:
            enviados    = int(float(row.get("registros_enviados", 0) or 0))
            confirmados = int(float(row.get("registros_confirmados", 0) or 0))
            if enviados > 0:
                perda_pct = (1 - confirmados / enviados) * 100
                completude_str = f"{confirmados}/{enviados}"
                perda_str = f"{perda_pct:.3f}%"
                # Alertar se perda > 0.1%
                if perda_pct > 0.1:
                    perda_str += " ⚠"
            else:
                completude_str = "N/A"
                perda_str = "N/A"

            print(f"{row[col_nivel]:<22} {float(row['throughput_ops_s']):>13,.0f} "
                  f"{float(row['lat_media_ms']):>11.2f} "
                  f"{float(row['lat_p95_ms']):>8.2f} "
                  f"{float(row['lat_p99_ms']):>8.2f} "
                  f"{completude_str:>14} {perda_str:>8}")
    else:
        # Formato v1 (retrocompatível)
        print(f"{'Nível/Concern':<25} {'Throughput':>15} {'Lat. Média':>14} "
              f"{'P95':>10} {'P99':>10}")
        print("-" * 78)
        for row in dados:
            print(f"{row[col_nivel]:<25} {float(row['throughput_ops_s']):>15,.0f} "
                  f"{float(row['lat_media_ms']):>14.2f} "
                  f"{float(row['lat_p95_ms']):>10.2f} "
                  f"{float(row['lat_p99_ms']):>10.2f}")

    print("=" * 78)


# ─── Auditoria de completude ────────────────────────────────────────────────

def auditar_completude(nome_banco: str, dados: list[dict],
                       col_nivel: str) -> None:
    """
    [C6] Verifica perda silenciosa de escritas por nível.
    Cassandra: upsert natural por chave (sensor_id, data_bucket, ts) → sem duplicatas.
    MongoDB TS: sem índice único → duplicatas possíveis em reenvio; alertar usuário.
    """
    if not dados or "registros_enviados" not in dados[0]:
        return

    print(f"\n  Auditoria de completude — {nome_banco}")
    print(f"  {'Nível':<22} {'Enviados':>10} {'Confirmados':>13} {'Perda':>10} {'Status':>10}")
    print("  " + "-" * 65)

    algum_problema = False
    for row in dados:
        env  = int(float(row.get("registros_enviados", 0) or 0))
        conf = int(float(row.get("registros_confirmados", 0) or 0))
        if env == 0:
            continue
        perda = (1 - conf / env) * 100
        status = "OK" if perda <= 0.1 else "⚠ PERDA"
        if perda > 0.1:
            algum_problema = True
        print(f"  {row[col_nivel]:<22} {env:>10,} {conf:>13,} "
              f"{perda:>9.3f}% {status:>10}")

    if not algum_problema:
        print("  → Nenhuma perda detectada (≤ 0,1%).")
    else:
        print("  → Perda detectada! Verifique timeouts e configuração de CL/WriteConcern.")

    if nome_banco == "MongoDB":
        print("\n  [Idempotência MongoDB] Time Series NÃO suporta índice único.")
        print("  Reenvio do mesmo lote insere DUPLICATAS. Tratar na aplicação.")
    else:
        print("\n  [Idempotência Cassandra] INSERT faz upsert pela PK — reenvio")
        print("  do mesmo (sensor_id, data_bucket, ts) SUBSTITUI o valor anterior.")


# ─── Comparação direta ───────────────────────────────────────────────────────

def comparar_quorum_majority(cass: list[dict], mdb: list[dict]) -> None:
    quorum   = next((r for r in cass if r.get("nivel") == "QUORUM"), None)
    majority = next((r for r in mdb  if "majority" in r.get("write_concern", "")), None)

    if not (quorum and majority):
        return

    print("\n" + "─" * 78)
    print("  COMPARAÇÃO DIRETA — Equilíbrio (Cassandra QUORUM vs MongoDB w=majority)")
    print("─" * 78)

    thr_c  = float(quorum["throughput_ops_s"])
    thr_m  = float(majority["throughput_ops_s"])
    lat_c  = float(quorum["lat_media_ms"])
    lat_m  = float(majority["lat_media_ms"])
    p99_c  = float(quorum["lat_p99_ms"])
    p99_m  = float(majority["lat_p99_ms"])

    winner_thr = "Cassandra" if thr_c > thr_m else "MongoDB"
    winner_lat = "Cassandra" if lat_c < lat_m else "MongoDB"

    print(f"  {'Métrica':<30} {'Cassandra QUORUM':>18} {'MongoDB w=majority':>18} {'Vencedor':>12}")
    print("  " + "-" * 78)
    print(f"  {'Throughput (ops/s)':<30} {thr_c:>18,.0f} {thr_m:>18,.0f} {winner_thr:>12}")
    print(f"  {'Latência leitura média (ms)':<30} {lat_c:>18.2f} {lat_m:>18.2f} {winner_lat:>12}")
    print(f"  {'Latência leitura P99 (ms)':<30} {p99_c:>18.2f} {p99_m:>18.2f}")
    print("─" * 78)
    print("  Ambos toleram queda de 1 nó sem perda de dados.")
    print("  Cassandra peer-to-peer: sem single point of failure na eleição.")
    print("  MongoDB: eleição de PRIMARY pode levar 10–30s de indisponibilidade.")


# ─── Entrypoint ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    base = os.path.join(os.path.dirname(__file__), "..", "resultados")

    cass = ler_csv(os.path.join(base, "cassandra_benchmark.csv"))
    mdb  = ler_csv(os.path.join(base, "mongodb_benchmark.csv"))

    imprimir_tabela("CASSANDRA v2 — por ConsistencyLevel", cass, "nivel")
    imprimir_tabela("MONGODB v2 — por WriteConcern",       mdb,  "write_concern")

    auditar_completude("Cassandra", cass, "nivel")
    auditar_completude("MongoDB",   mdb,  "write_concern")

    comparar_quorum_majority(cass, mdb)

    # Verificar se faltam CSVs (benchmark ainda não executado)
    faltam = []
    if not cass:
        faltam.append("cassandra_benchmark.csv → execute benchmark_cassandra.py")
    if not mdb:
        faltam.append("mongodb_benchmark.csv   → execute benchmark_mongodb.py")
    if faltam:
        print("\n[!] CSVs pendentes:")
        for f in faltam:
            print(f"    {f}")
