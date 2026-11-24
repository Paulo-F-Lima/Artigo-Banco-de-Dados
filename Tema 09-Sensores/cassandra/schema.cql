-- =============================================================
-- Schema Cassandra v2 — Fazenda de Sensores IoT
-- Inclui padrão Write-Through previsto na modelagem original.
-- Execute: cqlsh -f schema.cql  (ou pelo setup_cassandra.py)
-- =============================================================

CREATE KEYSPACE IF NOT EXISTS fazenda
WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 3
}
AND durable_writes = true;

USE fazenda;

-- =============================================================
-- Tabela principal de leituras
--
-- Partition Key: (sensor_id, data_bucket)
--   • data_bucket = 'YYYY-MM-DD' — Time-Bucketing por dia
--   • Limita cada partição a ≤ 86.400 linhas (bounded)
--   • Consulta cross-day: WHERE data_bucket IN ('YYYY-MM-DD', 'YYYY-MM-DD')
--     quando a janela de 1h atravessa a meia-noite UTC
--
-- Clustering Key: ts DESC
--   • Leitura da última hora é sequencial a partir do topo
--
-- Compaction: TimeWindowCompactionStrategy (TWCS)
--   • Agrupa SSTables da mesma janela de 1h para compactação eficiente
--   • Evita read-amplification em séries temporais com TTL
-- =============================================================

CREATE TABLE IF NOT EXISTS fazenda.leituras_sensores (
  sensor_id    int,
  data_bucket  text,
  ts           timestamp,
  temperatura  float,
  umidade      float,
  PRIMARY KEY ((sensor_id, data_bucket), ts)
) WITH CLUSTERING ORDER BY (ts DESC)
  AND compaction = {
      'class': 'TimeWindowCompactionStrategy',
      'compaction_window_unit': 'HOURS',
      'compaction_window_size': 1
  }
  AND default_time_to_live = 604800;  -- TTL 7 dias

-- =============================================================
-- Tabela auxiliar: última leitura por sensor
-- Padrão Write-Through [C3]:
--   Atualizar JUNTO com leituras_sensores a cada inserção.
--   Suporta dashboards em tempo real sem varredura na tabela principal.
--
-- Implementação no código Python:
--   Opção A (mesmo tick): BATCH não-logado
--     BEGIN UNLOGGED BATCH
--       INSERT INTO leituras_sensores (...) VALUES (...);
--       INSERT INTO ultima_leitura   (...) VALUES (...);
--     APPLY BATCH;
--
--   Opção B (escala alta): chamadas assíncronas paralelas
--     fut1 = session.execute_async(stmt_leitura, params1)
--     fut2 = session.execute_async(stmt_ultima,  params2)
--     fut1.result(); fut2.result()
--
--   BATCH não-logado é preferível para a mesma partição pois evita
--   o overhead do batchlog (journaling distribuído do LOGGED BATCH).
--   ATENÇÃO: não use LOGGED BATCH para partições diferentes —
--   o coordenador pode se tornar gargalo.
-- =============================================================

CREATE TABLE IF NOT EXISTS fazenda.ultima_leitura (
  sensor_id    int PRIMARY KEY,
  ts           timestamp,
  temperatura  float,
  umidade      float
);

-- Verificação
DESCRIBE TABLES;
