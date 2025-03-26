# ElasticSearch Sink

curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d '{
  "name": "elasticsearch-sink",
  "config": {
     "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
     "tasks.max": "1",
     "topics": "deltas.retail.sales",
     "key.ignore": "false",
     "connection.url": "http://elasticsearch:9200",
     "type.name": "sales",
     "name": "elasticsearch-sink",
     "transforms": "key",
     "transforms.key.type": "org.apache.kafka.connect.transforms.ExtractField$Key",
     "transforms.key.field": "id",
     "key.converter": "org.apache.kafka.connect.json.JsonConverter",
     "value.converter": "org.apache.kafka.connect.json.JsonConverter",
     "write.method": "UPSERT",
     "errors.tolerance": "all",
     "errors.deadletterqueue.topic.name": "dlq",
     "errors.deadletterqueue.topic.replication.factor": "1",
     "behavior.on.null.values": "delete"
  }
}'

# Post Image
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d '{
  "name": "images",
  "config": {
    "connector.class": "com.scylladb.cdc.debezium.connector.ScyllaConnector",
    "tasks.max": "1",
    "scylla.name": "cdc-demo",
    "scylla.cluster.ip.addresses": "scylla:9042",
    "scylla.table.names": "retail.sales",
    "kafka.bootstrap.servers": "kafka:9092",
    "start.from": "earliest",
    "scylla.query.time.window.size": "1000",
    "scylla.confidence.window.size": "100",
    "postimages.enabled": "true",
    "preimages.enabled": "true"
  }
}'

# Deltas
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d '{
  "name": "deltas",
  "config": {
    "connector.class": "com.scylladb.cdc.debezium.connector.ScyllaConnector",
    "tasks.max": "1",
    "scylla.name": "deltas",
    "scylla.cluster.ip.addresses": "scylla:9042",
    "scylla.table.names": "retail.sales",
    "scylla.query.time.window.size": "1000",
    "scylla.confidence.window.size": "100",
    "kafka.bootstrap.servers": "kafka:9092",
    "start.from": "earliest",
    "transforms": "NewRecordState",
    "transforms.NewRecordState.type": "com.scylladb.cdc.debezium.connector.transforms.ScyllaExtractNewRecordState",
    "transforms.NewRecordState.drop.tombstones": "false",
    "transforms.NewRecordState.delete.handling.mode": "drop"
  }
}'

    "transforms.NewRecordState.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.NewRecordState.drop.tombstones": "false",
    "transforms.NewRecordState.delete.handling.mode": "rewrite",
    "transforms.NewRecordState.add.fields": "op, source.ts_ms"
