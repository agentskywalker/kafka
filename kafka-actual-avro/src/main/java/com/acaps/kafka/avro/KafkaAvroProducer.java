package com.acaps.kafka.avro;


import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;


public class KafkaAvroProducer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final  Logger logger = LoggerFactory.getLogger(KafkaProducer.class);
        //String    bootstrapServers = "192.168.31.187:9092";
        String    bootstrapServers = "ec2-13-235-79-67.ap-south-1.compute.amazonaws.com:9092";

        Properties properties = new Properties();
        // normal producer
        //properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty("acks", "all");
        properties.setProperty("retries", "10");
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);

        // avro part
        //properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //properties.setProperty("value.serializer", StringSerializer.class.getName());

        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());


        //properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        //properties.setProperty("schema.registry.url", "http://192.168.31.187:8081");
        properties.setProperty("schema.registry.url", "http://ec2-13-235-79-67.ap-south-1.compute.amazonaws.com:8081");

        //properties.setProperty("KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG", "http://schema-registry:8081");




        //Create Producer

        KafkaProducer<String, AcapsApplication> producer = new KafkaProducer<String, AcapsApplication>(properties);

        String topic = "acaps-application-avro-1";

        // copied from avro examples
        AcapsApplication acapsApplication = AcapsApplication.newBuilder()
                .setApplRefNum("A190415000001")
                .setApplStatus("D10")
                .setFirstName("John")
                .setLastName("Doe")
                .build();

        ProducerRecord<String, AcapsApplication> producerRecord = new ProducerRecord<String, AcapsApplication>(
                topic, acapsApplication
        );

        //display the message
        System.out.println(acapsApplication);

        for(int i=0; i<10 ; i++) {


            String  key = "id_" + Integer.toString(i);
            //String  value   =   "Jyo Says Hi !!! " + Integer.toString(i);

            //Create Producer Record
            ProducerRecord<String, AcapsApplication> record = new ProducerRecord<String, AcapsApplication>(topic, acapsApplication);

            logger.info("Key: " + key); //log the key

            //check whether producer sed was a success or it failed
            producer.send(record, new Callback() {

                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        logger.info("Received Metadata. \n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offset: " + metadata.offset() + "\n" +
                                "Timestamp: " + metadata.timestamp());
                        System.out.println(metadata);
                    } else {
                        exception.printStackTrace();
                    }
                }

            }); //.get(); //block the .send() to make it synchronous - DON'T do this in production
        }


       //Flush and Close Producer
        producer.flush();
        producer.close();



    }
}
