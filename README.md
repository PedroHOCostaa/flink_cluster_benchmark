# Flink Cluster Benchmark
## Comandos para utilizar o Docker
### Entrar em container no modo bash
```bash
docker exec -it flink_jobmanager.1.8ki4bml93wi76pa2rriqoitn2 bash
```
### Copiar arquivo para container
```bash
docker cp kafka_test_job.py flink_jobmanager.1.8ki4bml93wi76pa2rriqoitn2:/opt/flink/jobs/
```
### Criar uma rede no swarm
```bash
docker network create -d overlay kafka_network
```
## Comandos para o Flink
### Rodar job python de fora do container
```bash
docker exec -it flink_jobmanager.1.ngsht9nh7bozqxgqxknwcjpi8 /opt/flink/bin/flink run -py /opt/flink/jobs/kafka_test_job.py
```
### Rodar job python de dentro do container
```bash
/opt/flink/bin/flink run -py /opt/flink/jobs/kafka_test_job.py
```
## Comandos para o Kafka
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
kafka-console-producer --topic test_topic --bootstrap-server localhost:9092
```

### Criar tópicos de produção consumo
```bash
kafka-topics --create --topic topico-entrada --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

```bash
kafka-topics --create --topic topico-saida --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```