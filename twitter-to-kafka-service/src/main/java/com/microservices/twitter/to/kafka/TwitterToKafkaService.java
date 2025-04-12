package com.microservices.twitter.to.kafka;

import com.microservices.twitter.to.kafka.config.TwitterToKafkaServiceConfigData;
import com.microservices.twitter.to.kafka.runner.StreamRunner;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@RequiredArgsConstructor
public class TwitterToKafkaService implements CommandLineRunner {

    private final StreamRunner streamRunner;
    public static final Logger LOGGER = LoggerFactory.getLogger(TwitterToKafkaService.class);
    private final TwitterToKafkaServiceConfigData configData;
    public static void main(String[] args) {
        SpringApplication.run(TwitterToKafkaService.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
    LOGGER.info(configData.getWelcomeMessage());
    LOGGER.info("TwitterToKafkaServiceConfigData: {}", configData.getTwitterKeywords().toString());
    streamRunner.start();
    }
}
