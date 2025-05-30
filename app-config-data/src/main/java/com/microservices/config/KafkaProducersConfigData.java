package com.microservices.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "kafka-producer-config")
public class KafkaProducersConfigData {
    private String keySerializeClass;
    private String valueSerializerClass;
    private String compressionType;
    private String acks;
    private int batchSize;
    private int batchSizeBootFactor;
    private int lingerMs;
    private int requestTimeoutMs;
    private int retryCount;
}
