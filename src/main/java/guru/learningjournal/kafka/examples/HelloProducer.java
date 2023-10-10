package guru.learningjournal.kafka.examples;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

public class HelloProducer {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        logger.info("Creating Properties.");
        Properties properties = new Properties();
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationID);
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, partition)

        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, AppConfigs.transactionId);


        logger.info("Creating Producer.");
        //KafkaProducer is supposed to transmit the record to the broker over the network, but it doesn't
        // immediately transfer the records. every record goes through serialization, partitioning,
        // and then it is kept in the buffer.
        KafkaProducer<Integer, String> producer = new KafkaProducer<>(properties);

        producer.initTransactions();
        logger.info("Starting first Transaction.");
        producer.beginTransaction();
        try {
            for (int i = 0; i < 10; i++) {
                //We package content in the ProducerRecord obj with at least two mandatory arguments.
                //kafka topic name - is the destination address of the message.
                // and message value - is the main content of the message.
                //You can also specify the following optional arguments
                //message key - is one of the most critical arguments, and is used for many purposes,
                //    such as partitioning, grouping and joins.
                //    it is a mandatory argument, but API doesn't  mandate it
                //target partition
                //message timestamp
                ProducerRecord<Integer, String> record1 = new ProducerRecord<>(AppConfigs.topicName1, i, "Message1 - " + i);
                ProducerRecord<Integer, String> record2 = new ProducerRecord<>(AppConfigs.topicName2, i, "Message2 - " + i);
                //hand over the record to KafkaProducer using send() method
                producer.send(record1);
                producer.send(record2);
            }
            logger.info("committing first transaction");
            producer.commitTransaction();
        } catch (Exception e) {
            logger.error("exception in first transaction. aborting...........");
            producer.abortTransaction();
            producer.close();
            throw new RuntimeException(e);
        }


        //rollback scenario
        logger.info("Starting second Transaction.");

        producer.beginTransaction();

        try {
            for (int i = 0; i < 10; i++) {
                ProducerRecord<Integer, String> record1 = new ProducerRecord<>(AppConfigs.topicName1, i, "Message3 - " + i);
                ProducerRecord<Integer, String> record2 = new ProducerRecord<>(AppConfigs.topicName2, i, "Message4 - " + i);
                //hand over the record to KafkaProducer using send() method
                producer.send(record1);
                producer.send(record2);
            }
            logger.info("Aborting...............");
            producer.abortTransaction();
        } catch (Exception e) {
            logger.error("exception in second transaction. aborting...........");
            producer.abortTransaction();
            producer.close();
            throw new RuntimeException(e);
        }
        logger.info("Messages are sent.");
        producer.close();
    }
}
