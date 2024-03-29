package guru.learningjournal.kafka.examples;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(Consumer.class);

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");//Broker
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());//Deserialization of messages and keys
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group2");//Consumer's group
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");//earliest, none

        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton(AppConfigs.topicName1));//Returns an immutable set containing only the specified object.
                                                                // The returned set is serializable.
                                                                //Params: o – the sole object to be stored in the returned set.
                                                                //Returns: an immutable set containing only the specified object.

        while(true){
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(100));//wait for and get a message
            for (ConsumerRecord<Integer, String> record: records) {
                logger.info("key " + record.key() + " value " + record.value() + " partition " +
                        record.partition() + " offset " + record.offset());
            }
        }


    }
}
