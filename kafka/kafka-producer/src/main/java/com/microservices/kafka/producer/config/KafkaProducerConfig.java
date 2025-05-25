package com.microservices.kafka.producer.config;

import com.microservices.config.KafkaConfigData;
import com.microservices.config.KafkaProducersConfigData;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig<X extends Serializable,V extends SpecificRecordBase> {

    private final KafkaConfigData kafkaConfigData;
    private final KafkaProducersConfigData kafkaProducersConfigData;

    public KafkaProducerConfig(KafkaConfigData kafkaConfigData, KafkaProducersConfigData kafkaProducersConfigData) {
        this.kafkaConfigData = kafkaConfigData;
        this.kafkaProducersConfigData = kafkaProducersConfigData;
    }

    @Bean
    public Map<String,Object> producerConfig() {
        Map<String,Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfigData.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, kafkaProducersConfigData.getKeySerializeClass());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, kafkaProducersConfigData.getValueSerializerClass());
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,kafkaProducersConfigData.getBatchSize() * kafkaProducersConfigData.getBatchSizeBootFactor());
        props.put(ProducerConfig.LINGER_MS_CONFIG,kafkaProducersConfigData.getLingerMs());
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,kafkaProducersConfigData.getCompressionType());
        props.put(kafkaConfigData.getSchemaRegistryUrlKey(), kafkaConfigData.getSchemaRegistryUrl());
        props.put(ProducerConfig.ACKS_CONFIG,kafkaProducersConfigData.getAcks());
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,kafkaProducersConfigData.getRequestTimeoutMs());
        props.put(ProducerConfig.RETRIES_CONFIG,kafkaProducersConfigData.getRetryCount());
        return  props;
    }

    @Bean
    public ProducerFactory<X , V> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfig());
    }

    @Bean
    public KafkaTemplate<X,V> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
