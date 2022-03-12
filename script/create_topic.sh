~/kafka/bin/kafka-topics.sh \
--create \
--zookeeper localhost:2181 \
--replication-factor 2 \
--partitions 3 \
--topic nyt.archive