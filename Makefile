DOCKER_USER = pedrohocostaa
REPO_NAME = iot-stream-flink-benchmark

FLINK_RASP = ./flink/rasp/
FLINK_TRAD = ./flink/tradicional/

build_all:
	docker buildx build --platform linux/arm64 -t $(DOCKER_USER)/$(REPO_NAME):rasp --load $(FLINK_RASP)
	docker build --no-cache -t $(DOCKER_USER)/$(REPO_NAME):trad $(FLINK_TRAD)
build_rasp:
	docker buildx build --platform linux/arm64 -t $(DOCKER_USER)/$(REPO_NAME):rasp --load $(FLINK_RASP)
build_trad:
	docker build --no-cache -t $(DOCKER_USER)/$(REPO_NAME):trad $(FLINK_TRAD)

push_all:
	docker push $(DOCKER_USER)/$(REPO_NAME):rasp
	docker push $(DOCKER_USER)/$(REPO_NAME):trad
push_rasp:
	docker push $(DOCKER_USER)/$(REPO_NAME):rasp
push_trad:
	docker push $(DOCKER_USER)/$(REPO_NAME):trad

on_rasp:
	docker stack deploy -c ./flink/rasp/flink-stack.yml flink
	docker stack deploy -c ./kafka/kafka-stack.yml kafka

on_trad:
	docker stack deploy -c ./flink/tradicional/flink-stack.yml flink
	docker stack deploy -c ./kafka/kafka-stack.yml kafka

off:
	docker stack rm flink
	docker stack rm kafka
