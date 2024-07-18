# Kafka

## Starting with Kafka

We will start kafka server using Docker <br>

1. Let's create kafka and Zookeeper
```bash
docker compose up -d
```

2. Creating kafka topic.
```bash
docker exec <container_id> kafka-topics --create --topic live-score-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

3. Verify the kafka
```bash
docker exec <container_id> kafka-topics --list --bootstrap-server localhost:9092
```