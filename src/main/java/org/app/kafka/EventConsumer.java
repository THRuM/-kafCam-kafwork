package org.app.kafka;

import org.app.domain.event.DataNotInCacheEvent;

public interface EventConsumer {
    void consume(DataNotInCacheEvent dataNotInCacheEvent);
}
