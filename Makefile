DOCKER_USER = pedrohocostaa
REPO_NAME = iot-stream-flink-benchmark

FLINK_LOCAL = ./flink/local/
FLINK_RASP = ./flink/rasp/
FLINK_TRAD = ./flink/tradicional/

build_all:
	docker build --no-cache -t $(DOCKER_USER)/$(REPO_NAME):local $(FLINK_LOCAL)
	docker buildx build --platform linux/arm64 -t $(DOCKER_USER)/$(REPO_NAME):rasp --load $(FLINK_RASP)
	docker build --no-cache -t $(DOCKER_USER)/$(REPO_NAME):trad $(FLINK_TRAD)
build_local:
	docker build --no-cache -t $(DOCKER_USER)/$(REPO_NAME):local $(FLINK_LOCAL)
build_rasp:
	docker buildx build --platform linux/arm64 -t $(DOCKER_USER)/$(REPO_NAME):rasp --load $(FLINK_RASP)
build_trad:
	docker build --no-cache -t $(DOCKER_USER)/$(REPO_NAME):trad $(FLINK_TRAD)

push_all:
	docker push $(DOCKER_USER)/$(REPO_NAME):local
	docker push $(DOCKER_USER)/$(REPO_NAME):rasp
	docker push $(DOCKER_USER)/$(REPO_NAME):trad
push_local:
	docker push $(DOCKER_USER)/$(REPO_NAME):local
push_rasp:
	docker push $(DOCKER_USER)/$(REPO_NAME):rasp
push_trad:
	docker push $(DOCKER_USER)/$(REPO_NAME):trad

clean:
	docker rmi $(DOCKER_USER)/$(REPO_NAME):local || true
	docker rmi $(DOCKER_USER)/$(REPO_NAME):rasp || true
	docker rmi $(DOCKER_USER)/$(REPO_NAME):trad || true

on_local:
	docker stack deploy -c ./flink/local/flink-stack.yml flink
	docker stack deploy -c ./kafka_riotbench/kafka-stack.yml kafka

on_rasp:
	docker stack deploy -c ./flink/rasp/flink-stack.yml flink
	docker stack deploy -c ./kafka_riotbench/kafka-stack.yml kafka

on_trad:
	docker stack deploy -c ./flink/tradicional/flink-stack.yml flink
	docker stack deploy -c ./kafka_riotbench/kafka-stack.yml kafka

off:
	docker stack rm flink
	docker stack rm kafka

swarm_start:
	docker swarm init
	docker network create --driver overlay kafka_network

swarm_leave:
	docker swarm leave --force

copy_jobs:
	docker cp ./flink/jobs $(docker ps -qf "name=flink_jobmanager"):/opt/flink/jobs/

kill:
	docker stack rm flink
	docker stack rm kafka
	docker swarm leave --force

network_start:
	docker network create -d overlay kafka_network
