# Kafka Lab (TP)

## Objectifs
- Producer / Consumer (console + Java)
- Kafka Connect (FileSource/FileSink)
- Kafka Streams : WordCount
- Cluster multi-brokers : réplication + failover

## Contenu du projet
- `src/main/java/edu/supmti/kafka/`
  - `EventProducer.java`
  - `EventConsumer.java`
  - `WordCountApp.java`
- `proof/` : sorties des commandes (preuves)

## Tests réalisés
### 1) Connect
- Source: `/tmp/test-source.txt` → Topic `connect-topic`
- Sink: Topic `connect-topic` → `/tmp/test-sink.txt`

### 2) Streams WordCount
- Input topic : `input-topic`
- Output topic : `output-topic`
- Exemple output : `hello : 2`, `kafka : 3`

### 3) Replication / Failover
- Topic: `tp-replication` (replication-factor=3)
- Test: arrêt d’un broker leader → le topic continue de fonctionner
