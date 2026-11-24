"""
teste_falha_no.py  (v2 — automatizado e com saída CSV para o TCC)
=================================================================
Mede a disponibilidade, a latência e a janela de indisponibilidade do
Apache Cassandra (RF=3) sob ConsistencyLevel QUORUM e ALL quando um nó
réplica (`cassandra-3`) é interrompido em tempo de execução, permitindo
comparação direta com `teste_falha_no_mongodb.py`.

Arquitetura do teste:
  - O driver conecta ao nó coordenador `cassandra-1` (127.0.0.1:9042).
  - Após 15 iterações de baseline (7,5 s), o script executa
    `docker stop cassandra-3` automaticamente via subprocess.
  - Em RF=3 com 2 nós ativos (`cassandra-1` e `cassandra-2`):
      * QUORUM (⌊3/2⌋ + 1 = 2 nós): continua operando sem interrupção
        (0,00 s de indisponibilidade, arquitetura peer-to-peer sem eleição).
      * ALL (3 nós obrigatórios): falha imediatamente com exceção
        `Unavailable` até a restauração de `cassandra-3`.
  - Ao final de cada nível, `cassandra-3` é restaurado (`docker start`) e
    o script aguarda o protocolo Gossip marcar os 3 nós como `UN` (Up/Normal)
    via `nodetool status` antes de iniciar o próximo teste.

Uso:
    python teste_falha_no.py

Saídas geradas em `resultados/`:
  - `falha_no_cassandra_QUORUM.csv`
  - `falha_no_cassandra_ALL.csv`
  - `falha_no_cassandra_resumo.csv`
"""

import csv
import random
import statistics
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy

# ─── Configurações ──────────────────────────────────────────────────────────
CASSANDRA_HOST  = "127.0.0.1"
KEYSPACE        = "fazenda"
SENSOR_ID       = 99
INTERVALO_SEG   = 0.5            # intervalo entre escritas (simétrico ao MongoDB)
ITERACOES_TOTAL = 80             # 80 × 0,5s = 40s por nível (igual ao MongoDB)
BASELINE_ITERS  = 15             # derruba o nó na iteração 16 (índice 15)
CONTAINER_ALVO  = "cassandra-3"  # réplica derrubada (mantém seed cassandra-1)
CONTAINER_SEED  = "cassandra-1"  # usado para checar gossip via nodetool


# ─── Helpers de Infraestrutura e Caminhos ───────────────────────────────────

def resolver_pasta_resultados() -> Path:
    """Resolve `resultados/` tanto a partir de `python/` quanto da raiz."""
    pasta_script = Path(__file__).resolve().parent
    if pasta_script.name == "python":
        pasta_res = pasta_script.parent / "resultados"
    else:
        pasta_res = pasta_script / "resultados"
    pasta_res.mkdir(parents=True, exist_ok=True)
    return pasta_res


def conectar(cl: int):
    cluster = Cluster(
        contact_points=[CASSANDRA_HOST],
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="dc1"),
        protocol_version=4,
        connect_timeout=5,
        control_connection_timeout=5,
    )
    session = cluster.connect(KEYSPACE)
    session.default_consistency_level = cl
    return cluster, session


def parar_container(nome: str) -> bool:
    """Derruba o container Docker via subprocess."""
    print(f"\n  [🛑] Derrubando {nome} via Docker...")
    res = subprocess.run(
        ["docker", "stop", nome],
        capture_output=True, text=True, timeout=20
    )
    if res.returncode == 0:
        print(f"  [✓] {nome} parado com sucesso.")
        return True
    print(f"  [!] Aviso ao parar {nome}: {res.stderr.strip()}")
    return False


def iniciar_e_aguardar_cluster(nome: str, timeout_seg: int = 60) -> None:
    """
    Restaura o container Docker e aguarda o protocolo Gossip do Cassandra
    reconhecer os 3 nós como UN (Up/Normal) antes do próximo cenário.
    """
    print(f"\n  [▶] Restaurando {nome}...")
    subprocess.run(["docker", "start", nome], capture_output=True, timeout=20)
    print(f"  [⏳] Aguardando Gossip estabilizar os 3 nós (UN)...")

    t0 = time.perf_counter()
    while time.perf_counter() - t0 < timeout_seg:
        res = subprocess.run(
            ["docker", "exec", CONTAINER_SEED, "nodetool", "status"],
            capture_output=True, text=True, timeout=10
        )
        if res.returncode == 0:
            nos_un = sum(
                1 for linha in res.stdout.splitlines()
                if linha.strip().startswith("UN ")
            )
            if nos_un >= 3:
                print(f"  [✓] Cluster íntegro ({nos_un}/3 nós UN) em "
                      f"{time.perf_counter() - t0:.1f}s.")
                return
        time.sleep(2)

    print(f"  [!] Tempo limite ({timeout_seg}s) atingido aguardando Gossip; "
          f"prosseguindo.")


# ─── Execução de um Nível de Consistência ───────────────────────────────────

def executar_teste(nome_nivel: str, cl: int,
                   iteracoes: int = ITERACOES_TOTAL) -> dict:
    """
    Ciclo de teste simétrico ao `teste_falha_no_mongodb.py`:
      1. Executa `BASELINE_ITERS` escritas com cluster íntegro (3/3 nós).
      2. Derruba `CONTAINER_ALVO` (`cassandra-3`) automaticamente.
      3. Continua enviando leituras até completar `iteracoes`, registrando
         sucesso/falha, tipo de exceção e latência por operação.
      4. Restaura `cassandra-3` e exporta CSV detalhado.
    """
    print(f"\n{'=' * 65}")
    print(f"  ConsistencyLevel: {nome_nivel}  |  Nó derrubado: {CONTAINER_ALVO}")
    print(f"{'=' * 65}")

    cluster, session = conectar(cl)
    prepared = session.prepare("""
        INSERT INTO leituras_sensores
            (sensor_id, data_bucket, ts, temperatura, umidade)
        VALUES (?, ?, ?, ?, ?)
    """)

    resultados_iter = []
    no_derrubado    = False
    t_queda         = None
    t_recuperacao   = None
    houve_falha_pos_queda = False

    try:
        for i in range(iteracoes):
            agora  = datetime.now(timezone.utc)
            bucket = agora.strftime("%Y-%m-%d")

            # Derruba o nó réplica após o baseline
            if i == BASELINE_ITERS and not no_derrubado:
                parar_container(CONTAINER_ALVO)
                no_derrubado = True
                t_queda = time.perf_counter()

            t0 = time.perf_counter()
            erro_str = ""
            try:
                session.execute(prepared, (
                    SENSOR_ID,
                    bucket,
                    agora,
                    round(random.uniform(15.0, 40.0), 2),
                    round(random.uniform(40.0, 95.0), 2),
                ))
                ok = True
            except Exception as e:
                ok = False
                erro_str = type(e).__name__
                if no_derrubado:
                    houve_falha_pos_queda = True

            lat = (time.perf_counter() - t0) * 1000

            # Detecta recuperação caso tenha havido indisponibilidade transitória
            if no_derrubado and houve_falha_pos_queda and ok and t_recuperacao is None:
                t_recuperacao = time.perf_counter()
                print(f"  [✓] Escrita recuperada em {t_recuperacao - t_queda:.2f}s!")

            status_str = "OK" if ok else f"FALHA ({erro_str})"
            resultados_iter.append({
                "iteracao":    i + 1,
                "ts":          agora.isoformat(),
                "ok":          ok,
                "latencia_ms": round(lat, 2),
                "no_caido":    no_derrubado,
                "erro":        erro_str,
            })
            print(f"  [{i+1:3d}] {status_str:<20} | {agora.strftime('%H:%M:%S')} | "
                  f"lat={lat:.1f}ms")

            time.sleep(INTERVALO_SEG)
    finally:
        cluster.shutdown()
        iniciar_e_aguardar_cluster(CONTAINER_ALVO)

    total    = len(resultados_iter)
    sucessos = sum(1 for r in resultados_iter if r["ok"])
    falhas   = total - sucessos

    lats_normal = [
        r["latencia_ms"] for r in resultados_iter
        if not r["no_caido"] and r["ok"]
    ]
    lats_degradada = [
        r["latencia_ms"] for r in resultados_iter
        if r["no_caido"] and r["ok"]
    ]

    if not houve_falha_pos_queda:
        janela_indisp = "0.00"
    elif t_queda and t_recuperacao:
        janela_indisp = f"{t_recuperacao - t_queda:.2f}"
    else:
        # Permaneceu indisponível durante toda a janela com o nó derrubado
        duracao_degradada = (iteracoes - BASELINE_ITERS) * INTERVALO_SEG
        janela_indisp = f">{duracao_degradada:.2f} (indisponivel)"

    metricas = {
        "consistency_level":      nome_nivel,
        "total_iteracoes":        total,
        "sucessos":               sucessos,
        "falhas":                 falhas,
        "taxa_disponibilidade":   f"{(sucessos / total * 100):.1f}%",
        "janela_indisponivel_s":  janela_indisp,
        "lat_media_normal_ms":    f"{statistics.mean(lats_normal):.2f}" if lats_normal else "N/D",
        "lat_media_degradada_ms": f"{statistics.mean(lats_degradada):.2f}" if lats_degradada else "N/D",
        "no_derrubado":           CONTAINER_ALVO,
    }

    print(f"\n  Resultado ({nome_nivel}): {sucessos}/{total} OK "
          f"({metricas['taxa_disponibilidade']}) | "
          f"Janela indisponível: {metricas['janela_indisponivel_s']}s")

    pasta_res = resolver_pasta_resultados()
    nome_csv  = pasta_res / f"falha_no_cassandra_{nome_nivel}.csv"
    with open(nome_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=resultados_iter[0].keys())
        writer.writeheader()
        writer.writerows(resultados_iter)
    print(f"  [✓] Log detalhado salvo em: {nome_csv}")

    return metricas


# ─── Comparativo Opcional com o Resumo do MongoDB ───────────────────────────

def exibir_comparativo_mongodb(pasta_res: Path, metricas_cass: list[dict]) -> None:
    """Se `falha_no_mongodb_resumo.csv` existir, exibe comparação lado a lado."""
    csv_mdb = pasta_res / "falha_no_mongodb_resumo.csv"
    if not csv_mdb.exists():
        return

    with open(csv_mdb, encoding="utf-8") as f:
        rows_mdb = list(csv.DictReader(f))

    if not rows_mdb:
        return

    print("\n" + "=" * 72)
    print("  COMPARATIVO DE DISPONIBILIDADE SOB FALHA DE NÓ (CASSANDRA × MONGODB)")
    print("=" * 72)
    print(f"{'SGBD / Política':<26} {'Sucessos':>10} {'Disponib.':>12} {'Janela Indisp.':>20}")
    print("-" * 72)
    for m in metricas_cass:
        rotulo = f"Cassandra ({m['consistency_level']})"
        sucessos_str = f"{m['sucessos']}/{m['total_iteracoes']}"
        print(f"{rotulo:<26} {sucessos_str:>10} {m['taxa_disponibilidade']:>12} "
              f"{m['janela_indisponivel_s'] + 's':>20}")
    for m in rows_mdb:
        rotulo = f"MongoDB ({m['write_concern']})"
        sucessos_str = f"{m['sucessos']}/{m['total_iteracoes']}"
        print(f"{rotulo:<26} {sucessos_str:>10} {m['taxa_disponibilidade']:>12} "
              f"{m['janela_indisponivel_s'] + 's':>20}")
    print("=" * 72)


# ─── Ponto de Entrada ───────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 65)
    print("  TESTE DE FALHA DE NÓ v2 — Apache Cassandra (RF=3)")
    print("=" * 65)
    print(f"  1. Escreve continuamente a cada {INTERVALO_SEG}s ({ITERACOES_TOTAL} iterações)")
    print(f"  2. Derruba {CONTAINER_ALVO} automaticamente após {BASELINE_ITERS} iterações")
    print(f"  3. Contrasta QUORUM (peer-to-peer tolerante) vs ALL (consistência estrita)")
    print(f"  4. Restaura {CONTAINER_ALVO} e aguarda Gossip antes do próximo nível\n")

    input("Pressione ENTER para iniciar o teste com QUORUM...")
    m_quorum = executar_teste("QUORUM", ConsistencyLevel.QUORUM)

    input("\nPressione ENTER para iniciar o teste com ALL...")
    m_all = executar_teste("ALL", ConsistencyLevel.ALL)

    print("\n" + "=" * 65)
    print("  RESUMO — CASSANDRA FAILOVER (RF=3)")
    print("=" * 65)
    for m in [m_quorum, m_all]:
        print(f"\n  ConsistencyLevel: {m['consistency_level']}")
        print(f"    Disponibilidade:        {m['taxa_disponibilidade']} "
              f"({m['sucessos']}/{m['total_iteracoes']})")
        print(f"    Janela indisponível:    {m['janela_indisponivel_s']}s")
        print(f"    Lat. média (normal):    {m['lat_media_normal_ms']} ms")
        print(f"    Lat. média (degradada): {m['lat_media_degradada_ms']} ms")

    pasta_res = resolver_pasta_resultados()
    caminho_resumo = pasta_res / "falha_no_cassandra_resumo.csv"
    with open(caminho_resumo, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=m_quorum.keys())
        writer.writeheader()
        writer.writerow(m_quorum)
        writer.writerow(m_all)
    print(f"\n[✓] Resumo salvo em: {caminho_resumo}")

    exibir_comparativo_mongodb(pasta_res, [m_quorum, m_all])