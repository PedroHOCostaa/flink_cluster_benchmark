# flink_cluster_benchmark
### Entrar em container no modo bash
```bash
docker exec -it flink_jobmanager.1.8ki4bml93wi76pa2rriqoitn2 bash
```
### Copiar arquivo para container
```bash
docker cp kafka_test_job.py flink_jobmanager.1.8ki4bml93wi76pa2rriqoitn2:/opt/flink/jobs/
```
### Executar código Python pelo flink
```bash
/opt/flink/bin/flink run -py /opt/flink/jobs/kafka_test_job.py
```