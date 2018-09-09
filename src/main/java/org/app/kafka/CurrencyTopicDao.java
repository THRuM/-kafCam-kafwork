package org.app.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.app.domain.Currency;
import org.app.domain.event.CannotCompleteHistoryEvent;
import org.app.domain.event.CompleteHistoryEvent;
import org.app.domain.event.DataNotInCacheEvent;
import org.app.mongo.CurrencyData;
import org.app.mongo.RequestDataWrapper;
import org.app.mongo.RequestDataWrapperMongoStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Named
public class CurrencyTopicDao {

    private RequestDataWrapperMongoStore requestDataWrapperMongoStore;
    private KafkaConsumer<String, Currency> consumer;
    private EventSender eventSender;

    @Value("${currency.topic}")
    private String currencyTopic;

    @Value("${notification.topic}")
    private String notificationEventTopic;

    @Value("${spring.kafka.consumer.bootstrap-servers}")
    private String kafkaServers;

//    @Value("${spring.kafka.properties.security.protocol}")
//    private String kafkaSecurityProtocol;
//
//    @Value("${spring.kafka.properties.sasl.mechanism}")
//    private String kafkaSaslMechanizm;
//
//    @Value("${spring.kafka.properties.sasl.jaas.config}")
//    private String kafkaJaasConfig;

    private final static long POOL_TIMEOUT = 1000;

    @Inject
    public CurrencyTopicDao(RequestDataWrapperMongoStore requestDataWrapperMongoStore,
                            EventSender eventSender) {
        this.requestDataWrapperMongoStore = requestDataWrapperMongoStore;
        this.eventSender = eventSender;
    }

    @PostConstruct
    public void init() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafkaWork" + UUID.randomUUID().toString());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "kafkaWorkClient" + UUID.randomUUID().toString());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
//        props.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, kafkaSecurityProtocol);
//        props.put(SaslConfigs.SASL_MECHANISM, kafkaSaslMechanizm);
//        props.put(SaslConfigs.SASL_JAAS_CONFIG, kafkaJaasConfig);

        props.put(JsonDeserializer.TRUSTED_PACKAGES, "org.app.domain");
        this.consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList(currencyTopic));
    }

    private static Predicate<Currency> isCurrencySymbolValid(String currencySymbol) {
        return currency -> currency.getSymbol().equalsIgnoreCase(currencySymbol);
    }

    private static Predicate<Currency> isTimeStampValid(Long timeStamp) {
        return currency -> currency.getTimestamp() < timeStamp;
    }

    void handleRequest(DataNotInCacheEvent dataNotInCacheEvent) {

        if (dataNotInCacheEvent == null) {
            eventSender.send(new CannotCompleteHistoryEvent("DataNotInCache is null", "0000",
                    new DataNotInCacheEvent("0000", "NULL", 1L)), notificationEventTopic);
            return;
        }

        Collection<Currency> fromKafkaTopic = new ArrayList<>();

        //Dummy poll because methods are lazy
        consumer.poll(POOL_TIMEOUT);

        Set<TopicPartition> partitions = consumer.assignment();

        //Go to end for that moment
        consumer.seekToEnd(partitions);

        Map<TopicPartition, Long> maxPositionsForPartitions = getLastPositionsForPartition(consumer, partitions, currencyTopic);

        consumer.seekToBeginning(partitions);

        try {
            while (true) {
                ConsumerRecords<String, Currency> records = consumer.poll(POOL_TIMEOUT);

                Map<TopicPartition, Long> actualPositionsForPartitions = getLastPositionsForPartition(consumer, partitions, currencyTopic);

                if (records.isEmpty())
                    break;

                records.forEach(rec -> {
                    Currency cur = rec.value();
                    if (isCurrencySymbolValid(dataNotInCacheEvent.getCurrencySymbol())
                            .and(isTimeStampValid(dataNotInCacheEvent.getTimeStamp()))
                            .test(cur)) {
                        fromKafkaTopic.add(cur);
                    }
                });

                boolean isAllRead = true;

                for (Map.Entry<TopicPartition, Long> partitionEntry : actualPositionsForPartitions.entrySet()) {
                    TopicPartition partition = partitionEntry.getKey();
                    Long offset = partitionEntry.getValue();

                    if (offset < maxPositionsForPartitions.get(partition)) {
                        isAllRead = false;
                    }
                }

                if (isAllRead)
                    break;
            }
        } catch (WakeupException e) {
            // ignore for shutdown
        }

        if (fromKafkaTopic.isEmpty()) {
            CannotCompleteHistoryEvent cannotEvent = new CannotCompleteHistoryEvent(
                    "No data on topic for request", dataNotInCacheEvent.getRequestId(), dataNotInCacheEvent);

            eventSender.send(cannotEvent, notificationEventTopic);
            return;
        }

        requestDataWrapperMongoStore.save(new RequestDataWrapper(dataNotInCacheEvent.getRequestId(), fromKafkaTopic
                .stream()
                .map(CurrencyData::new)
                .collect(Collectors.toList())));

        eventSender.send(new CompleteHistoryEvent(dataNotInCacheEvent.getRequestId()), notificationEventTopic);
    }

    private Map<TopicPartition, Long> getLastPositionsForPartition(KafkaConsumer consumer, Set<TopicPartition> partitions, String partition) {

        List<TopicPartition> partitionsForTopic = partitions.stream()
                .filter(t -> t.topic().equalsIgnoreCase(partition)).collect(Collectors.toList());

        return partitionsForTopic.stream()
                .collect(Collectors.toMap(t -> t, consumer::position));
    }

    @PreDestroy
    public void shutdown() {
        consumer.close();
    }
}
