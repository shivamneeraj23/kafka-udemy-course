package kafka.course.tutorial;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;


public class ConsumerDemo {
    public static void main(String[] args) {
        Logger logger= LoggerFactory.getLogger(ConsumerDemo.class.getName());
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-fourth-application-test-1-1";
        String topic = "first_topic";
        // create consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // DESERIALIZATION: Kafka sends this byte directly to consumer, it takes these bytes and create a string from
        // it.
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // auto offset rest config values -> earliest/latest/none
        // earliest: read from the beginning of the topic.
        // latest: read from the new messages onwards.
        //none: will thrown an error.
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        // subscribe consumer to our topics
        // by doing Collection.singleton() we  only subscibe to one topic, although we can subscribe to
        // many topics
        // subscribe to multiple topics using: Arrays.asList(<List of topics>)
        consumer.subscribe(Collections.singleton(topic));

        // poll for new data
        while (true){
            //consumer.poll(100);// new in Kafka 2.0.0 // deprecated
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record: records){
                logger.info("Key: "+ record.key() + ", Values: "+ record.value());
                logger.info("Partition: " + record.partition()+ ", Offset: "+ record.offset());
            }
        }

    }

}
