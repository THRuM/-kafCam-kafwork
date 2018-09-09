package org.app.mongo;

import org.springframework.data.mongodb.repository.MongoRepository;

public interface RequestDataWrapperMongoStore extends MongoRepository<RequestDataWrapper, String> {
}
