package demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    public static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {
        log.info("In ConsumerDemo Class");

        String groupId = "my-java-application";
        String topic = "demo_java";

        //create Producer Properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "https://happy-bison-10891-eu2-kafka.upstash.io:9092");
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"aGFwcHktYmlzb24tMTA4OTEkZCz_f3RbsbOCMnEdB8P-nUSPjXPidEougm5Vdbs\" password=\"NzYxODA0NmItNzU4OS00YTdhLThlMGUtZGJhMmZmNjZlOTg3\";");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // create consumer config
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");


        //create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //subscribe to a topic
        consumer.subscribe(Arrays.asList(topic));

        //poll for data
        while (true) {
            log.info("Polling");

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {
                log.info("Key: " + record.key() + ", Value: " + record.value());
                log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
            }
        }

    }
}
