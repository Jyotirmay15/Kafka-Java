package demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    public static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    public static void main(String[] args) {
        log.info("In ProducerDemoWithCallback Class");

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

//        properties.setProperty("batch.size", "400");

        //create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int j = 0; j < 10; j++) {

            for (int i = 0; i < 30; i++) {
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "Hello World" + i);

                //send the data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        //executes every time a record is successfully sent or an exception is thrown
                        if (e == null) {
                            //the record was successfully sent
                            log.info("Received new metadata \n" +
                                    "Topic: " + metadata.topic() + "\n" +
                                    "Partition: " + metadata.partition() + "\n" +
                                    "Offeset: " + metadata.offset() + "\n" +
                                    "Timestamp: " + metadata.timestamp());
                        } else {
                            log.error("Error while producing ", e);
                        }
                    }
                });
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }


        //create a Producer Record

        //tell the producer to send all data and block until done --synchronous
        producer.flush();

        //flush and close the producer
        producer.close();

    }
}
