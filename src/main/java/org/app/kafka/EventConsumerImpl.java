package org.app.kafka;

import org.app.domain.event.DataNotInCacheEvent;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;

import javax.inject.Inject;
import javax.inject.Named;

@Named
@KafkaListener(topics = "${worker.topic}")
public class EventConsumerImpl implements EventConsumer {

    private CurrencyTopicDao currencyTopicDao;

    @Inject
    public EventConsumerImpl(CurrencyTopicDao currencyTopicDao) {
        this.currencyTopicDao = currencyTopicDao;
    }

    @Override
    @KafkaHandler
    public void consume(DataNotInCacheEvent dataNotInCacheEvent) {
        currencyTopicDao.handleRequest(dataNotInCacheEvent);
    }
}
