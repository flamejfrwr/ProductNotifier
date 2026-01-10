package by.javaguru.ws.emailnotification;

import by.javaguru.ws.core.ProductCreatedEvent;
import by.javaguru.ws.emailnotification.handler.ProductCreatedEventHandler;
import by.javaguru.ws.emailnotification.persistance.entity.ProcessedEventEntity;
import by.javaguru.ws.emailnotification.repository.ProcessedEventRepository;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.*;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ActiveProfiles("test")
@EmbeddedKafka
@SpringBootTest(properties = {"spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}", "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}"})
public class ProductCreatedEventHandlerIntegrationTest {

    @Autowired
    KafkaTemplate<String, Object> kafkaTemplate;

    @Mock
    ProcessedEventRepository processedEventRepository;

    @Mock
    RestTemplate restTemplate;

    @MockitoSpyBean
    ProductCreatedEventHandler productCreatedEventHandler;

    @Test
    public void testProductCreatedEventHandler_OnProduct_Created_HandlesEvent() throws ExecutionException, InterruptedException {
        //Arrange
        ProductCreatedEvent productCreatedEvent = new ProductCreatedEvent();
        productCreatedEvent.setPrice(new BigDecimal(10));
        productCreatedEvent.setProductId(UUID.randomUUID().toString());
        productCreatedEvent.setQuantity(1);
        productCreatedEvent.setTitle("Test Product");

        String messageId = UUID.randomUUID().toString();
        String messageKey = productCreatedEvent.getProductId();

        ProducerRecord<String, Object> record = new ProducerRecord<>(
                "product-created-events-topic",messageKey,productCreatedEvent
        );

        record.headers().add("messageId", messageId.getBytes());
        record.headers().add(KafkaHeaders.RECEIVED_KEY, messageKey.getBytes());

        ProcessedEventEntity processedEventEntity = new ProcessedEventEntity();
        when(processedEventRepository.findByMessageId(anyString())).thenReturn(processedEventEntity);
        when(processedEventRepository.save(any(ProcessedEventEntity.class))).thenReturn(null);

        String responseBody = "{\"key\":\"value\"}";
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        ResponseEntity<String> responseEntity = new ResponseEntity<>(responseBody, headers, HttpStatus.OK);
        //isNull() чисто возвращает null
        when(restTemplate.exchange(
                any(String.class),
                any(HttpMethod.class),
                isNull(), eq(String.class))).thenReturn(responseEntity);
        //Act
        kafkaTemplate.send(record).get();
        //Assert
        ArgumentCaptor<String> messageIdCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> messageKeyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ProductCreatedEvent> eventCaptor = ArgumentCaptor.forClass(ProductCreatedEvent.class);

        verify(productCreatedEventHandler, timeout(5000).times(1)).handle(eventCaptor.capture(), messageIdCaptor.capture(), messageKeyCaptor.capture());

        assertEquals(messageId, messageIdCaptor.getValue());
        assertEquals(messageKey, messageKeyCaptor.getValue());
        assertEquals(productCreatedEvent.getProductId(), eventCaptor.getValue().getProductId());
    }
}
