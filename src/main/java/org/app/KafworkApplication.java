package org.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@SpringBootApplication
@EnableMongoRepositories
public class KafworkApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafworkApplication.class, args);
    }
}
