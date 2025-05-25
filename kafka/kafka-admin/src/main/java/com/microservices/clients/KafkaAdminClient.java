package com.microservices.clients;

import com.microservices.config.KafkaConfigData;
import com.microservices.config.RetryConfigData;
import com.microservices.config.WebClientConfig;
import com.microservices.exceptions.KafkaClientException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Component
public class KafkaAdminClient {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaAdminClient.class);
    private final KafkaConfigData kafkaConfigData;
    private final RetryConfigData retryConfigData;
    private final AdminClient adminClient;
    private final RetryTemplate retryTemplate;
    private final WebClient webClient;

    public KafkaAdminClient(KafkaConfigData kafkaConfigData, RetryConfigData retryConfigData, AdminClient adminClient, RetryTemplate retryTemplate,
                            WebClient  webClient) {
        this.kafkaConfigData = kafkaConfigData;
        this.retryConfigData = retryConfigData;
        this.adminClient = adminClient;
        this.retryTemplate = retryTemplate;
        this.webClient =  webClient;
    }

    public void createTopics(){
        CreateTopicsResult createTopicsResult;

        try {
            createTopicsResult = retryTemplate.execute(this::doCreateTopics);

        }catch (Throwable t){
            throw new KafkaClientException("Reached the maximum number of attempts", t);
        }
        checkTopicCreated();

    }

    private void checkTopicCreated() {
        Collection<TopicListing> topics = getTopics();
        int retries = 1;
        int maxRetries = retryConfigData.getMaxAttempts();
        int multiple = retryConfigData.getMultiplier().intValue();
        Long sleepTimeMs = retryConfigData.getSleepTimeMs();

        for (TopicListing topic : topics) {
            while(!isTopicsCreated(topics,topic)){
                checkMaxRetries(retries++,maxRetries);
                sleep(sleepTimeMs);
                sleepTimeMs += multiple;
                topics = getTopics();
            }

        }
    }


    private void checkSchemaRegistryRunning(){
        int retries = 1;
        int maxRetries = retryConfigData.getMaxAttempts();
        int multiple = retryConfigData.getMultiplier().intValue();
        Long sleepTimeMs = retryConfigData.getSleepTimeMs();

        while (getSchemaRegistryStatus().is2xxSuccessful()){
            checkMaxRetries(retries++,maxRetries);
            sleep(sleepTimeMs);
            sleepTimeMs += multiple;
        }

    }
    private HttpStatusCode getSchemaRegistryStatus(){
        try {
            return webClient.method(HttpMethod.GET)
                    .uri(kafkaConfigData.getSchemaRegistryUrl())
                    .exchange()
                    .map(ClientResponse::statusCode)
                    .block();
        }catch (Throwable t){
            return HttpStatusCode.valueOf(500);
        }
    }

    private void checkMaxRetries(int i, int maxRetries) {
        if(i > maxRetries){
            throw new KafkaClientException("Maximum number of retries reached");
        }
    }

    private boolean isTopicsCreated(Collection<TopicListing> topics, TopicListing topic) {
        if(topics == null ){
            return false;
        }
        return topics.stream().anyMatch(t -> t.name().equals(topic.name()));
    }

    private void sleep(long sleepTimeMs) {

        try {
            Thread.sleep(sleepTimeMs);
        } catch (Throwable t) {
            throw new KafkaClientException("error while sleeping", t);
        }
    }


    private CreateTopicsResult doCreateTopics(RetryContext retryContext) {
        List<String> topicNames = kafkaConfigData.getTopicNamesToCreate();
        LOG.info("Creating topic names: {}", topicNames);

        List<NewTopic> kafkaTopics = topicNames.stream().map(topic -> new NewTopic(topic.trim(),kafkaConfigData.
                getNumOfPartitions(),
                (short) kafkaConfigData.getReplicationFactor()
        )).toList();

        return adminClient.createTopics(kafkaTopics);

    }

    private Collection<TopicListing> getTopics(){
        Collection<TopicListing> topicsListing;
        try {
            topicsListing = retryTemplate.execute(this::doGetTopics);
        }catch (Throwable kafkaClientException) {
            throw new KafkaClientException("Reached the maximum number of attempts", kafkaClientException);

        }
        return topicsListing;

    }

    private Collection<TopicListing> doGetTopics(RetryContext retryContext) throws ExecutionException, InterruptedException {
        LOG.info("Reading {} ,attempts {} ", kafkaConfigData.getTopicNamesToCreate().toArray(), retryContext.getRetryCount());
        Collection<TopicListing> topicsListings = adminClient.listTopics().listings().get();

        if(topicsListings != null){
            topicsListings.forEach(topic -> {
                LOG.debug("topic with Name {}",topic.name());
            });
        }
        return topicsListings;
    }


}
