!# usr/bin/bash

wget https://downloads.apache.org/kafka/3.8.0/kafka_2.13-3.8.0.tgz

tar -xzf kafka_2.13-3.8.0.tgz

cd kafka_2.13-3.8.0

KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"

bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties

bin/kafka-server-start.sh config/kraft/server.properties

