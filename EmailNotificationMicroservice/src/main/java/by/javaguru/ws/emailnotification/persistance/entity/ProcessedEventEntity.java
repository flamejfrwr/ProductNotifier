package by.javaguru.ws.emailnotification.persistance.entity;

import jakarta.persistence.*;

@Entity
@Table(name="processed_events")
public class ProcessedEventEntity {

    @Id
    @GeneratedValue
    private long id;

    @Column(nullable = false, unique = true)
    private String messageId;

    @Column(nullable = false)
    private String productId;

    public ProcessedEventEntity() {
    }

    public ProcessedEventEntity(String messageId, String productId) {
        this.messageId = messageId;
        this.productId = productId;
    }

    public long getId() {
        return id;
    }

    public String getMessageId() {
        return messageId;
    }

    public String getProductId() {
        return productId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }
}
