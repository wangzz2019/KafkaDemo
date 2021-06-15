# KafkaDemo

# Start zookeeper
cd /opt/apache-zookeeper-3.7.0-bin
bin/zkServer.sh start

# Stop zookeeper
bin/zkServer.sh stop

# Start Kafka
cd /opt/kafka_2.13-2.8.0
bin/kafka-server-start.sh config/server.properties 

# what is kafka:
https://datadoghq.atlassian.net/wiki/spaces/TS/pages/328664635/Kafka+basics

# List topics:
bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# Create topic:
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test

# Produce/consume message
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic topic-name
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic topic-name

# Supposed Model:
Service A (Java Spring boot). --> Kafka <-- Service B (Python)

