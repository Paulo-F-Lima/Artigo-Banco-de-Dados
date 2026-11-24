"""
setup_cassandra.py
==================
Inicializa o keyspace e a tabela no cluster Cassandra.
Execute DEPOIS que o cluster estiver UP (nodetool status = 3x UN).

Uso:
    python setup_cassandra.py
"""

import time
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy


def aguardar_cassandra(hosts: list[str], max_tentativas: int = 20) -> Cluster:
    """Tenta conectar ao Cassandra com retry exponencial."""
    for tentativa in range(1, max_tentativas + 1):
        try:
            cluster = Cluster(
                contact_points=hosts,
                load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="dc1"),
                protocol_version=4,
            )
            session = cluster.connect()
            print(f"[✓] Cassandra conectado na tentativa {tentativa}.")
            return cluster, session
        except Exception as e:
            espera = min(5 * tentativa, 30)
            print(f"[{tentativa}/{max_tentativas}] Aguardando Cassandra... ({e}). "
                  f"Próxima tentativa em {espera}s.")
            time.sleep(espera)
    raise RuntimeError("Não foi possível conectar ao Cassandra após várias tentativas.")


def criar_schema(session) -> None:
    """Cria keyspace e tabelas idempotentemente."""

    print("[*] Criando keyspace 'fazenda'...")
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS fazenda
        WITH replication = {
            'class': 'SimpleStrategy',
            'replication_factor': 3
        }
        AND durable_writes = true
    """)

    session.set_keyspace("fazenda")

    print("[*] Criando tabela 'leituras_sensores'...")
    session.execute("""
        CREATE TABLE IF NOT EXISTS leituras_sensores (
            sensor_id    int,
            data_bucket  text,
            ts           timestamp,
            temperatura  float,
            umidade      float,
            PRIMARY KEY ((sensor_id, data_bucket), ts)
        )
        WITH CLUSTERING ORDER BY (ts DESC)
          AND compaction = {
              'class': 'TimeWindowCompactionStrategy',
              'compaction_window_unit': 'HOURS',
              'compaction_window_size': 1
          }
    """)

    print("[*] Criando tabela auxiliar 'ultima_leitura'...")
    session.execute("""
        CREATE TABLE IF NOT EXISTS ultima_leitura (
            sensor_id   int PRIMARY KEY,
            ts          timestamp,
            temperatura float,
            umidade     float
        )
    """)

    print("[✓] Schema criado com sucesso.")


if __name__ == "__main__":
    cluster, session = aguardar_cassandra(["127.0.0.1"])
    try:
        criar_schema(session)
    finally:
        cluster.shutdown()
