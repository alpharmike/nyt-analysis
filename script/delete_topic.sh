TOPIC_NAME=$1

kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic ${TOPIC_NAME}