package demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    public static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);

    public static void main(String[] args) {
        log.info("In ProducerDemo Class");

        //create Producer Properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "https://happy-bison-10891-eu2-kafka.upstash.io:9092");
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"aGFwcHktYmlzb24tMTA4OTEkZCz_f3RbsbOCMnEdB8P-nUSPjXPidEougm5Vdbs\" password=\"NzYxODA0NmItNzU4OS00YTdhLThlMGUtZGJhMmZmNjZlOTg3\";");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        //set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //create a Producer Record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "Hello World");

        //send the data
        producer.send(producerRecord);

        //tell the producer to send all data and block until done --synchronous
        producer.flush();

        //flush and close the producer
        producer.close();

    }
}
