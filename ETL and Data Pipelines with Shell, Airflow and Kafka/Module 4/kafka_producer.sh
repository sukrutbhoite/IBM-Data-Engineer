!# usr/bin/bash

cd kafka_2.13-3.8.0

bin/kafka-topics.sh --create --topic news --bootstrap-server localhost:9092

bin/kafka-console-producer.sh   --bootstrap-server localhost:9092   --topic news

