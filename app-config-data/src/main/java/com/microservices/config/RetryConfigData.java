package com.microservices.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "retry-config")
public class RetryConfigData {

    private Long initialIntervalMs;
    private Long maxIntervalMs;
    private Integer maxAttempts;
    private Double multiplier;
    private Long sleepTimeMs;

}
