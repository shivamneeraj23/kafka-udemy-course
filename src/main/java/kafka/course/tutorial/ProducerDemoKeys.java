package kafka.course.tutorial;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
        String bootstrapServers  = "127.0.0.1:9092";
        // create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        for (int i=0; i<10; i++) {
            String topic = "first_topic";
            String key = "id_" +Integer.toString(i);
            String value = "Hello World " + Integer.toString(i);
            logger.info(String.format("Key, %s", key));
            // by providing the key we gurantee, all the values with the same key will always go to same partition
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
            // send data - asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes every time a record is succesfully sent or an exception is thrown
                    if (e == null) {
                        // the record was succesfully send
                        logger.info("Recieved new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            }).get(); //block the .send() to make it synchronous - don;t do this in production;
        }
        // flush the data - done to wait for the data to be produced, before exiting. Otherwise the program exits.
        producer.flush();
        // flush and close the data
        producer.close();
    }
}

