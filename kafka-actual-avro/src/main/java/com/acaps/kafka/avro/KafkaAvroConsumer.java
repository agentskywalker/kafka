package com.acaps.kafka.avro;


import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class KafkaAvroConsumer {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(KafkaAvroConsumer.class);
        //String bootstrapServers = "kafka.jyoti.com:9092";  //[main] WARN org.apache.kafka.clients.ClientUtils - Couldn't resolve server kafka.jyoti.com:9092 from bootstrap.servers as DNS resolution failed for kafka.jyoti.com
        String bootstrapServers = "192.168.31.187:9092";
        String groupID = "JyoApplication";
        String topic = "acaps-application-avro-1";

        //Create Consumer configs
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // avro part (deserializer)
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.setProperty("schema.registry.url", "http://192.168.31.187:8081");
        properties.setProperty("specific.avro.reader", "true");

        //Create Consumer
        KafkaConsumer<String, AcapsApplication> consumer = new KafkaConsumer<String, AcapsApplication>(properties);

        //Subscribe consumer to our topic(s)
        consumer.subscribe(Arrays.asList(topic));

        logger.info("Waiting for data...");

        while (true) {
            System.out.println("Polling");
            ConsumerRecords<String, AcapsApplication> records = consumer.poll(Duration.ofMillis(100)); //new in Kafka 2.0

            for (ConsumerRecord<String, AcapsApplication> record : records) {
                /*logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());*/

                AcapsApplication acapsApplication = record.value();
                System.out.println(acapsApplication);
            }

            consumer.commitSync();
        }
    }
}
