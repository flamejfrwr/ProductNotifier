package by.javaguru.ws.emailnotification.handler;

import by.javaguru.ws.core.ProductCreatedEvent;
import by.javaguru.ws.emailnotification.exception.NonRetryableException;
import by.javaguru.ws.emailnotification.exception.RetryableException;
import by.javaguru.ws.emailnotification.persistance.entity.ProcessedEventEntity;
import by.javaguru.ws.emailnotification.repository.ProcessedEventRepository;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

@Component
@KafkaListener(topics = "product-created-events-topic", containerFactory = "productCreatedEventKafkaListener")
public class ProductCreatedEventHandler {

    private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

    private RestTemplate restTemplate;
    private ProcessedEventRepository processedEventRepository;

    public ProductCreatedEventHandler(RestTemplate restTemplate, ProcessedEventRepository processedEventRepository) {
        this.restTemplate = restTemplate;
        this.processedEventRepository = processedEventRepository;
    }
    @Transactional
    @KafkaHandler
    public void handle(@Payload ProductCreatedEvent productCreatedEvent,
                       @Header("messageId") String messageId,
                       @Header(KafkaHeaders.RECEIVED_KEY) String messageKey){
        LOGGER.info("Received event: {}, productId: {}", productCreatedEvent.getTitle(), productCreatedEvent.getProductId());

        ProcessedEventEntity processedEventEntity = processedEventRepository.findByMessageId(messageId);

        if(processedEventEntity != null){
            LOGGER.info("Duplicate message: {}", messageId);
            return;
        }

        try {
            String url = "http://localhost:8090/response/200";
            ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.GET, null, String.class);
            if(response.getStatusCode().value() == HttpStatus.OK.value()){
                LOGGER.info("Received response: {}", response.getBody());
            }
        } catch (ResourceAccessException e){
            LOGGER.error(e.getMessage());
            throw new RetryableException(e);
        } catch (HttpServerErrorException e){
            LOGGER.error(e.getMessage());
            throw new NonRetryableException(e);
        } catch (Exception e){
            LOGGER.error(e.getMessage());
            throw new NonRetryableException(e);
        }

        try {
            processedEventRepository.save(new ProcessedEventEntity(messageId, messageKey));
        } catch (DataIntegrityViolationException e){
            LOGGER.error(e.getMessage());
            throw new NonRetryableException(e);
        }

    }

}
