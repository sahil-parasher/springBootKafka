package org.example;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@EmbeddedKafka(
        topics = "my_topic",
        partitions = 3,
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
class KafkaApplicationTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @SpyBean
    private KafkaApplication kafkaApplication;

    @Test
    void publishesMessageAndConsumesIt() throws Exception {
        String message = "Kafka producer-consumer test";

        kafkaTemplate.send("my_topic", message).get(10, TimeUnit.SECONDS);

        verify(kafkaApplication, timeout(10_000)).consume(message);
    }
}
