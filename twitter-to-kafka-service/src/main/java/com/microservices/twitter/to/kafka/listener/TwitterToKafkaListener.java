package com.microservices.twitter.to.kafka.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.StatusAdapter;

@Component
public class TwitterToKafkaListener extends StatusAdapter {

    private static final Logger logger = LoggerFactory.getLogger(TwitterToKafkaListener.class);

    @Override
    public void onStatus(Status status) {
        logger.info("Twitter status with text {}", status.getText());

    }
}
