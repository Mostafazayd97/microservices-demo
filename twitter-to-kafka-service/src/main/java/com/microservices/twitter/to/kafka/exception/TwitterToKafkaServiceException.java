package com.microservices.twitter.to.kafka.exception;

public class TwitterToKafkaServiceException extends RuntimeException {

    public TwitterToKafkaServiceException() {
    }

    public TwitterToKafkaServiceException(String message, Throwable cause) {
        super(message, cause);
    }

    public TwitterToKafkaServiceException(String message) {
        super(message);
    }

}
