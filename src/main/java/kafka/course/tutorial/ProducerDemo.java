package kafka.course.tutorial;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        String bootstrapServers  = "127.0.0.1:9092";
        // create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        // create a producer record
        ProducerRecord <String, String> record =  new ProducerRecord<String, String>("first_topic",
                "Hey Testing");
        // send data - asynchronous
        producer.send(record);
        // flush the data - done to wait for the data to be produced, before exiting. Otherwise the program exits.
        producer.flush();
        // flush and close the data
        producer.close();
    }
}
