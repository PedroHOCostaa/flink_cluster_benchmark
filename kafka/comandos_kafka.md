###  Criar tópicos
```bash
kafka-topics --create --topic test_topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```
### Listar tópicos
```bash
kafka-topics --list --bootstrap-server localhost:9092
```
### Produzir em um tópico
```bash
kafka-console-producer --bootstrap-server localhost:9092 --topic test_topic
```
### Consumir em um tópico
```bash
kafka-console-consumer --topic test_topic --bootstrap-server localhost:9092
```

### Tópicos Utilizados no teste
```bash
kafka-topics --create --topic topic-clima-entrada --bootstrap-server localhost:9092 --partitions 12 --replication-factor 1
kafka-topics --create --topic topic-clima-saida --bootstrap-server localhost:9092 --partitions 12 --replication-factor 1
```

