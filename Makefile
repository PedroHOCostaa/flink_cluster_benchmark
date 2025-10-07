# Construir as imagens dos containers
build:
	docker build -t flink-with-kafka:2.0 ./flink/
	docker build -t riotbench ./kafka_riotbench/
# Iniciar o ambiente de desenvolvimento
on:
	docker stack deploy -c ./flink/flink-stack.yml flink
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
	docker leave --force

# Criar e aplicar migrações
copy_jobs:
	docker cp ./jobs $(docker ps -qf "name=flink_jobmanager"):/opt/flink/jobs/

kill:
	docker stack rm flink
	docker stack rm kafka
	docker swarm leave --force

network_start:
	docker network create -d overlay kafka_network
