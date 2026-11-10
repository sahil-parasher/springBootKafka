package org.example;

import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@RestController
public class KafkaApplication {

    private static final Logger logger = LoggerFactory.getLogger(KafkaApplication.class);
    private final KafkaTemplate<String, String> kafkaTemplate;
    private static final String TOPIC_NAME = "my_topic";

    public KafkaApplication(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public static void main(String[] args) {
        SpringApplication.run(KafkaApplication.class, args);
    }

    // 1. Automatically create the topic on startup
    @Bean
    public NewTopic compactTopic() {
        return TopicBuilder.name(TOPIC_NAME)
                .partitions(3)
                .replicas(1)
                .build();
    }

    // 2. HTTP Endpoint to trigger the Producer
    @GetMapping("/publish")
    public String sendMessageToKafka(@RequestParam("message") String message) {
        this.kafkaTemplate.send(TOPIC_NAME, message);
        return "Message sent successfully to Kafka!";
    }

    // 3. Consumer Listener to receive the messages
    @KafkaListener(topics = TOPIC_NAME, groupId = "my-group-id")
    public void consume(String message) {
        logger.info("#### -> Consumed message -> {}", message);
    }
}
