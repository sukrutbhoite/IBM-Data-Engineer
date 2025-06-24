!# usr/bin/bash

cd kafka_2.13-3.8.0

bin/kafka-console-consumer.sh   --bootstrap-server localhost:9092   --topic news   --from-beginning

