package guru.learningjournal.kafka.examples;

class AppConfigs {
    final static String applicationID = "HelloProducer";
    final static String bootstrapServers = "localhost:9092,localhost:9093";// host/port pair
    final static String topicName1 = "transactional-topic-1";
    final static String topicName2 = "transactional-topic-2";
    final static int numEvents = 1_000_000;

    final static String transactionId = "aTransactionId";
}
