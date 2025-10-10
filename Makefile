# Construir as imagens dos containers
build:
	docker build -t flink-with-kafka-local:2.0 ./flink/local/
	docker build -t flink-with-kafka-rasp:2.0 ./flink/rasp/
	docker build -t flink-with-kafka-trad:2.0 ./flink/tradicional/

# Iniciar o ambiente de desenvolvimento
on_local:
	docker stack deploy -c ./flink/local/flink-stack.yml flink
	docker stack deploy -c ./kafka_riotbench/kafka-stack.yml kafka

# Iniciar o ambiente de desenvolvimento
on_rasp:
	docker stack deploy -c ./flink/rasp/flink-stack.yml flink
	docker stack deploy -c ./kafka_riotbench/kafka-stack.yml kafka

# Iniciar o ambiente de desenvolvimento
on_trad:
	docker stack deploy -c ./flink/tradicional/flink-stack.yml flink
	docker stack deploy -c ./kafka_riotbench/kafka-stack.yml kafka

# Restaurar o ambiente (apaga tudo e reconstrói)
off:
	docker stack rm flink
	docker stack rm kafka

# Subir containers manualmente
swarm_start:
	docker swarm init
	docker network create --driver overlay kafka_network

swarm_leave:
	docker swarm leave --force

# Criar e aplicar migrações
copy_jobs:
	docker cp ./jobs $(docker ps -qf "name=flink_jobmanager"):/opt/flink/jobs/

kill:
	docker stack rm flink
	docker stack rm kafka
	docker swarm leave --force

network_start:
	docker network create -d overlay kafka_network
