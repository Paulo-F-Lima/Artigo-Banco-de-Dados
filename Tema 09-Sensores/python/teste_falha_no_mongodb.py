"""
teste_falha_no_mongodb.py
=========================
Mede o tempo de eleição de novo PRIMARY quando um nó MongoDB cai,
comparando com o comportamento peer-to-peer do Cassandra (que não tem
single point of failure na eleição).

Pré-requisito: adicionar mongo1/mongo2/mongo3 → 127.0.0.1 no hosts.
  Execute setup_hosts_admin.bat como Administrador UMA VEZ antes deste script.

Uso:
  1. Suba o cluster MongoDB: docker compose up -d  (pasta mongodb/)
  2. Execute este script em um terminal
  3. Em outro terminal, execute: docker stop mongo1
  4. Observe: eleição automática do novo PRIMARY + tempo de indisponibilidade

    python teste_falha_no_mongodb.py

Saída: resultados/falha_no_mongodb.csv
"""

import csv
import os
import random
import subprocess
import time
from datetime import datetime, timezone, timedelta

from pymongo import MongoClient
from pymongo.errors import (
    ConnectionFailure, ServerSelectionTimeoutError,
    NotPrimaryError, AutoReconnect
)
from pymongo.write_concern import WriteConcern

# ─── Configurações ──────────────────────────────────────────────────────────
# URI com todos os membros para que o driver descubra o novo PRIMARY após eleição
# Requer que mongo1/mongo2/mongo3 estejam no arquivo hosts do Windows
MONGO_URI_RS = (
    "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/"
    "?replicaSet=rs0&serverSelectionTimeoutMS=5000"
)
# Fallback: se hosts não configurado, usa directConnection ao novo PRIMARY
MONGO_URI_FALLBACK = "mongodb://localhost:27018/?directConnection=true"

DB_NAME         = "fazenda"
COL_NAME        = "teste_falha"
INTERVALO_SEG   = 0.5     # intervalo entre escritas
SENSOR_TESTE    = 999
CONTAINER_ALVO  = "mongo1"   # nó que será derrubado (PRIMARY por padrão)


def conectar_rs() -> MongoClient:
    """Tenta conectar via Replica Set URI (precisa de hosts configurado)."""
    try:
        client = MongoClient(MONGO_URI_RS, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        print("[✓] Conectado via Replica Set URI (hosts configurado).")
        return client
    except Exception:
        print("[!] URI de RS falhou. Tentando fallback directConnection para mongo2...")
        client = MongoClient(MONGO_URI_FALLBACK, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        print("[✓] Conectado via fallback.")
        return client


def obter_primary(client: MongoClient) -> str:
    """Retorna host:porta do PRIMARY atual."""
    try:
        status = client.admin.command("replSetGetStatus")
        for m in status.get("members", []):
            if m.get("stateStr") == "PRIMARY":
                return m.get("name", "desconhecido")
    except Exception:
        pass
    return "desconhecido"


def escrever_leitura(col, ts: datetime, w=1) -> bool:
    """Tenta inserir um documento. Retorna True se bem-sucedido."""
    doc = {
        "sensor_id":   SENSOR_TESTE,
        "ts":          ts,
        "temperatura": round(random.uniform(15.0, 40.0), 2),
        "umidade":     round(random.uniform(40.0, 95.0), 2),
    }
    try:
        col.with_options(write_concern=WriteConcern(w=w)).insert_one(doc)
        return True
    except (NotPrimaryError, AutoReconnect, ConnectionFailure,
            ServerSelectionTimeoutError):
        return False


def parar_container(nome: str) -> None:
    """Derruba o container Docker via subprocess."""
    print(f"\n  [🛑] Derrubando {nome}...")
    result = subprocess.run(
        ["docker", "stop", nome],
        capture_output=True, text=True, timeout=15
    )
    if result.returncode == 0:
        print(f"  [✓] {nome} parado.")
    else:
        print(f"  [!] Erro ao parar {nome}: {result.stderr.strip()}")


def iniciar_container(nome: str) -> None:
    """Restaura o container Docker."""
    print(f"\n  [▶] Restaurando {nome}...")
    subprocess.run(["docker", "start", nome], capture_output=True, timeout=15)
    time.sleep(5)
    print(f"  [✓] {nome} restaurado.")


def executar_teste(w_nivel: int | str, label: str,
                   iteracoes: int = 80) -> dict:
    """
    Ciclo de teste:
      1. Escreve por 15 iterações (baseline OK)
      2. Derruba o container PRIMARY (mongo1)
      3. Continua escrevendo — mede janela de indisponibilidade
      4. Detecta quando eleição terminou e escrita volta
      5. Restaura o nó caído
    Retorna dicionário com métricas de tempo.
    """
    print(f"\n{'=' * 60}")
    print(f"  WriteConcern: {label}  |  Nó derrubado: {CONTAINER_ALVO}")
    print(f"{'=' * 60}")

    client = conectar_rs()
    db  = client[DB_NAME]

    # Recria coleção de teste (sem TS — precisamos de índice único para auditoria)
    if COL_NAME in db.list_collection_names():
        db[COL_NAME].drop()
    col = db.create_collection(COL_NAME)

    primary_antes = obter_primary(client)
    print(f"  PRIMARY antes: {primary_antes}")

    resultados_iter = []
    no_derrubado    = False
    t_queda         = None
    t_recuperacao   = None
    baseline_iters  = 15

    for i in range(iteracoes):
        agora = datetime.now(timezone.utc)

        # Derruba o nó após o baseline
        if i == baseline_iters and not no_derrubado:
            parar_container(CONTAINER_ALVO)
            no_derrubado = True
            t_queda = time.perf_counter()

        t0  = time.perf_counter()
        ok  = escrever_leitura(col, agora, w=w_nivel)
        lat = (time.perf_counter() - t0) * 1000

        status_str = "OK" if ok else "FALHA"

        # Detecta recuperação: primeira escrita OK após a queda
        if no_derrubado and ok and t_recuperacao is None:
            t_recuperacao = time.perf_counter()
            primary_depois = obter_primary(client)
            print(f"  [✓] Eleição concluída! Novo PRIMARY: {primary_depois}")
            print(f"  [⏱] Tempo de indisponibilidade: "
                  f"{t_recuperacao - t_queda:.2f}s")

        resultados_iter.append({
            "iteracao":    i + 1,
            "ts":          agora.isoformat(),
            "ok":          ok,
            "latencia_ms": round(lat, 2),
            "no_caido":    no_derrubado,
        })
        print(f"  [{i+1:3d}] {status_str:5} | {agora.strftime('%H:%M:%S')} | "
              f"lat={lat:.1f}ms")

        time.sleep(INTERVALO_SEG)

    # Restaura o nó
    iniciar_container(CONTAINER_ALVO)
    client.close()

    total      = len(resultados_iter)
    sucessos   = sum(1 for r in resultados_iter if r["ok"])
    falhas     = total - sucessos
    janela_s   = (t_recuperacao - t_queda) if (t_queda and t_recuperacao) else None

    metricas = {
        "write_concern":        label,
        "total_iteracoes":      total,
        "sucessos":             sucessos,
        "falhas":               falhas,
        "taxa_disponibilidade": f"{(sucessos / total * 100):.1f}%",
        "janela_indisponivel_s": f"{janela_s:.2f}" if janela_s else "N/D",
        "primary_antes":        primary_antes,
    }

    print(f"\n  Resultado: {sucessos}/{total} OK | "
          f"Janela indisponível: {metricas['janela_indisponivel_s']}s")

    # Salva CSV detalhado
    os.makedirs("../resultados", exist_ok=True)
    nome_csv = f"../resultados/falha_no_mongodb_{label.replace('=', '').replace(' ', '_')}.csv"
    with open(nome_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=resultados_iter[0].keys())
        writer.writeheader()
        writer.writerows(resultados_iter)
    print(f"  [✓] Log salvo em {nome_csv}")

    return metricas


if __name__ == "__main__":
    print("=" * 60)
    print("  TESTE DE FALHA DE NÓ — MongoDB Replica Set")
    print("  Pré-requisito: setup_hosts_admin.bat executado como Admin")
    print("=" * 60)
    print(f"\n  Este script:")
    print(f"  1. Escreve continuamente no MongoDB (Primary: mongo1)")
    print(f"  2. Derruba mongo1 após {15} iterações")
    print(f"  3. Mede janela de indisponibilidade até nova eleição")
    print(f"  4. Restaura mongo1 ao final")
    print(f"\n  Compare com teste_falha_no.py (Cassandra QUORUM):")
    print(f"  Cassandra não tem eleição — failover é transparente.\n")

    input("Pressione ENTER para iniciar o teste com w=majority...")
    m1 = executar_teste("majority", "w=majority")

    input("\nPressione ENTER para iniciar o teste com w=1...")
    m2 = executar_teste(1, "w=1")

    # Resumo comparativo
    print("\n" + "=" * 60)
    print("  RESUMO — MONGODB FAILOVER")
    print("=" * 60)
    for m in [m1, m2]:
        print(f"\n  WriteConcern: {m['write_concern']}")
        print(f"    Disponibilidade: {m['taxa_disponibilidade']}")
        print(f"    Janela indisponível: {m['janela_indisponivel_s']}s")
        print(f"    Falhas: {m['falhas']}/{m['total_iteracoes']}")

    print("\n  Cassandra (peer-to-peer): 0s de indisponibilidade com QUORUM")
    print("  pois não há PRIMARY único — qualquer nó coordena a escrita.")

    # Salva resumo
    with open("../resultados/falha_no_mongodb_resumo.csv", "w",
              newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=m1.keys())
        writer.writeheader()
        writer.writerow(m1)
        writer.writerow(m2)
    print("\n[✓] Resumo salvo em resultados/falha_no_mongodb_resumo.csv")
