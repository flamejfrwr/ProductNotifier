package by.javaguru.ws.productmicroservice;

import by.javaguru.ws.core.ProductCreatedEvent;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.util.Map;
@EmbeddedKafka
@SpringBootTest(properties = "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}")
public class IdempotentProducerIntegrationTest {

    @Autowired
    KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate;

    @Mock
    KafkaAdmin kafkaAdmin;

    @Test
    void testProducerConfig_whenIdempotenceEnabled_assertsIdempotentProperties(){
        //Arrange
        ProducerFactory<String, ProductCreatedEvent> producerFactory = kafkaTemplate.getProducerFactory();
        //Act
        Map<String, Object> config = producerFactory.getConfigurationProperties();
        //Assert
        Assertions.assertEquals("true", config.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
        Assertions.assertTrue("all".equalsIgnoreCase((String) config.get(ProducerConfig.ACKS_CONFIG)));
        if(config.get(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION) != null) {
            Assertions.assertTrue(Integer.parseInt(config.get(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION).toString()) <= 5);
        }
        if(config.containsKey(ProducerConfig.RETRIES_CONFIG)){
            Assertions.assertTrue(Integer.parseInt(config.get(ProducerConfig.RETRIES_CONFIG).toString()) > 0);
        }
    }
}
