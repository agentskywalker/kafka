package com.acaps.kafka.avro;


import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KafkaAvroConsumerThread {
    public static void main(String[] args) {

        new KafkaAvroConsumerThread().run();
    }

    private KafkaAvroConsumerThread(){}

    private void    run() {

        final Logger logger = LoggerFactory.getLogger(KafkaAvroConsumerThread.class);
        //String bootstrapServers = "192.168.31.187:9092";
        String    bootstrapServers = "ec2-13-235-79-67.ap-south-1.compute.amazonaws.com:9092";
        String groupId = "JyoApplication";
        String topic = "acaps-application-avro-1";

        // latch for dealing with multiple threads

        CountDownLatch latch = new CountDownLatch(1);

        // create the consumer runnable
        logger.info("Creating the consumer thread");
        Runnable myConsumerRunnable = new ConsumerRunnable(
                bootstrapServers,
                groupId,
                topic,
                latch
        );

        // start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }

        ));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }

    }

    public class ConsumerRunnable implements Runnable {

        private CountDownLatch latch;
        KafkaConsumer<String, AcapsApplication> consumer;

        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        public ConsumerRunnable(String bootstrapServers,
                                String groupId,
                                String topic,
                                CountDownLatch latch) {
            this.latch = latch;

            //Create Consumer configs
            Properties properties = new Properties();
            properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            // avro part (deserializer)
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
            //properties.setProperty("schema.registry.url", "http://192.168.31.187:8081");
            properties.setProperty("schema.registry.url", "http://ec2-13-235-79-67.ap-south-1.compute.amazonaws.com:8081");
            properties.setProperty("specific.avro.reader", "true");

            //Create Consumer
            consumer = new KafkaConsumer<String, AcapsApplication>(properties);

            //Subscribe consumer to our topic(s)
            consumer.subscribe(Arrays.asList(topic));
        }



        @Override
        public void run() {
            // poll for new data
            try {
                while (true) {
                    ConsumerRecords<String, AcapsApplication> records =
                            consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0

                    for (ConsumerRecord<String, AcapsApplication> record : records) {
                        logger.info("Key: " + record.key() + ", Value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());

                        AcapsApplication acapsApplication = record.value();
                        System.out.println(acapsApplication);
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal!");
            } finally {
                consumer.close();
                // tell our main code we're done with the consumer
                latch.countDown();
            }
        }

        public void shutdown() {
            // the wakeup() method is a special method to interrupt consumer.poll()
            // it will throw the exception WakeUpException
            consumer.wakeup();
        }
    }
}
