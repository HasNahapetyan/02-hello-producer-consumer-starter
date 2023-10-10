
C:\server\confluent-7.5.0\bin\windows\kafka-topics.bat --create --bootstrap-server localhost:9092 --topic transactional-topic-2 --partitions 5 --replication-factor 3 --config min.insync.replicas=2