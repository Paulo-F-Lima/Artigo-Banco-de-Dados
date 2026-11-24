"""
setup_mongodb.py
================
Inicializa o banco, a Time Series Collection e os índices no MongoDB.
Execute DEPOIS que o Replica Set estiver inicializado (rs.status() OK).

Uso:
    python setup_mongodb.py

Estratégia de conexão (Windows + Docker Desktop):
  - O RS é configurado internamente com mongo1/mongo2/mongo3 (rede Docker)
  - O Python no host conecta com directConnection=true ao PRIMARY (localhost:27017)
  - Isso evita que o driver tente resolver mongo1/mongo2/mongo3 no Windows
"""

import time
from pymongo import MongoClient
from pymongo.errors import CollectionInvalid, ServerSelectionTimeoutError


# ── URI com directConnection: conecta direto ao PRIMARY sem descoberta RS ──
# Se o PRIMARY mudar, basta reconectar ao localhost:27017 (ainda mapeado)
MONGO_URI_DIRECT = "mongodb://localhost:27017/?directConnection=true"


def aguardar_mongodb(uri: str, max_tentativas: int = 20) -> MongoClient:
    """Tenta conectar ao MongoDB com retry."""
    for tentativa in range(1, max_tentativas + 1):
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            print(f"[✓] MongoDB conectado na tentativa {tentativa}.")
            return client
        except ServerSelectionTimeoutError as e:
            espera = min(5 * tentativa, 30)
            print(f"[{tentativa}/{max_tentativas}] Aguardando MongoDB... "
                  f"Próxima tentativa em {espera}s.")
            time.sleep(espera)
    raise RuntimeError("Não foi possível conectar ao MongoDB após várias tentativas.")


def criar_schema(client: MongoClient) -> None:
    """Cria banco, coleção Time Series e índices."""
    db = client["fazenda"]

    # --- Time Series Collection -------------------------------------------
    colecoes_existentes = db.list_collection_names()
    if "leituras_sensores" in colecoes_existentes:
        print("[!] Coleção 'leituras_sensores' já existe. Pulando criação.")
    else:
        print("[*] Criando Time Series Collection 'leituras_sensores'...")
        db.create_collection(
            "leituras_sensores",
            timeseries={
                "timeField":   "ts",
                "metaField":   "sensor_id",
                "granularity": "seconds",
            },
            expireAfterSeconds=604800,   # TTL 7 dias
        )
        print("[✓] Time Series Collection criada.")

    # --- Índice auxiliar --------------------------------------------------
    print("[*] Criando índice composto (sensor_id, ts)...")
    db["leituras_sensores"].create_index(
        [("sensor_id", 1), ("ts", -1)],
        background=True,
        name="idx_sensor_ts",
    )
    print("[✓] Índice criado.")

    # --- Verificação ------------------------------------------------------
    info = db.list_collection_names()
    print(f"[✓] Coleções no banco 'fazenda': {info}")


if __name__ == "__main__":
    client = aguardar_mongodb(MONGO_URI_DIRECT)
    try:
        criar_schema(client)
    finally:
        client.close()
