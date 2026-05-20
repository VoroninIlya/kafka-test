```
kubectl exec -it kafka-0 -n kafka -- bash
```

```
ls /opt/kafka/bin
```

```
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-internal:9092 \
  --create \
  --topic test-topic \
  --partitions 3 \
  --replication-factor 3

/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic test-topic

/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

/opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server localhost:9092 \
  describe --status
```

```
kubectl run kafka-consumer -n kafka -it --rm \
  --image=apache/kafka:3.7.0 \
  -- bash
  
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-internal:9092 \
  --topic test-topic \
  --from-beginning
```

```
kubectl run kafka-producer -n kafka -it --rm \
  --image=apache/kafka:3.7.0 \
  -- bash
  
/opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka-internal:9092 \
  --topic test-topic
```
from host:
```
sudo apt update
sudo apt install openjdk-17-jdk -y
```

```
nc -vz 10.48.168.177 9094
nc -vz 192.168.1.103 9094
```

```
curl -LO https://archive.apache.org/dist/kafka/3.7.0/kafka_2.13-3.7.0.tgz
```

```
kafka-console-consumer.sh \
    --bootstrap-server 10.48.168.177:9094 \
    --topic test-topic \
    --from-beginning

kafka-console-consumer.sh \
    --bootstrap-server 192.168.1.103:9094 \
    --topic test-topic \
    --from-beginning
```

```
kubectl port-forward svc/kafka-ui 8080:80 -n kafka

http://localhost:8080
```