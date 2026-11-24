// =============================================================
// Schema MongoDB v2 — Fazenda de Sensores IoT
// Correção [C5]: índice explícito REMOVIDO.
//   MongoDB 7.0 Time Series cria automaticamente um índice
//   clusterizado composto em {sensor_id: 1, ts: 1} (metaField + timeField).
//   Criar {sensor_id: 1, ts: -1} manualmente gerava overhead duplo
//   de escrita nos buckets do WiredTiger sem benefício de leitura.
//
// Execute no mongosh conectado ao PRIMARY:
//   mongosh "mongodb://localhost:27017/?directConnection=true"
// =============================================================

use fazenda;

// ── Time Series Collection ────────────────────────────────────
// timeField  = "ts"        → campo ISO Date obrigatório
// metaField  = "sensor_id" → agrupa documentos do mesmo sensor
// granularity= "seconds"   → otimiza bucketing para IoT 1 Hz
// expireAfterSeconds       → TTL 7 dias (evita crescimento ilimitado)
// ─────────────────────────────────────────────────────────────
db.createCollection("leituras_sensores", {
  timeseries: {
    timeField:   "ts",
    metaField:   "sensor_id",
    granularity: "seconds"
  },
  expireAfterSeconds: 604800
});

// [C5] NÃO criar índice adicional aqui.
// O índice clusterizado nativo já cobre a consulta:
//   db.leituras_sensores.find({ sensor_id: 42, ts: { $gte: ... } })
// Verificar com:
//   db.leituras_sensores.explain("executionStats").find(
//     { sensor_id: 42, ts: { $gte: ISODate("2026-11-17T00:00:00Z") } }
//   )

// ── Verificação ───────────────────────────────────────────────
print("Coleções criadas:");
db.getCollectionInfos({ name: "leituras_sensores" });

print("\nÍndices da coleção (nativo clusterizado):");
db.leituras_sensores.getIndexes();
