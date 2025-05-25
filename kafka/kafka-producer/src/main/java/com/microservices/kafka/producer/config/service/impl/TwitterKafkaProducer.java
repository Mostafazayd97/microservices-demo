package com.microservices.kafka.producer.config.service.impl;

import com.microservices.kafka.avro.model.TwitterAvroModel;
import com.microservices.kafka.producer.config.service.KafkaProducer;
import org.glassfish.jersey.internal.guava.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class TwitterKafkaProducer implements KafkaProducer<Long, TwitterAvroModel> {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterKafkaProducer.class);

    private KafkaTemplate<Long, TwitterAvroModel> kafkaTemplate;


    public TwitterKafkaProducer(KafkaTemplate<Long, TwitterAvroModel> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void send(String topicName, Long key, TwitterAvroModel message) {
        LOG.info("Sending message to topic " + topicName + " with key " + key);
        CompletableFuture<SendResult<Long,TwitterAvroModel>> kafkaFuture = kafkaTemplate.send(topicName, key, message);



    }
}
