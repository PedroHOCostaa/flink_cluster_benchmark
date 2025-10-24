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

### Tópico de entrada
```bash
kafka-topics --create --topic topico-entrada --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### Tópico de saída
```bash
kafka-topics --create --topic topico-saida --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

