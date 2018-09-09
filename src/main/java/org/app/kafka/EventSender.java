package org.app.kafka;

import org.app.domain.event.DomainEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import javax.inject.Inject;
import javax.inject.Named;

@Named
public class EventSender {
    private static final Logger log = LoggerFactory.getLogger(EventSender.class);

    private KafkaTemplate<String, DomainEvent> kafkaTemplate;


    @Inject
    public EventSender(KafkaTemplate<String, DomainEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    void send(DomainEvent kafkaEvent, String topic) {
        log.info("event [{}] published to topic {}", kafkaEvent.getClass(), topic);
        kafkaTemplate.send(topic, kafkaEvent.getRequestId(), kafkaEvent);
    }

}
