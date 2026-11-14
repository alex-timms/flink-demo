package kafka;

import org.testcontainers.kafka.ConfluentKafkaContainer;

public class Container {

    public static ConfluentKafkaContainer createKafkaContainer() {
        return new ConfluentKafkaContainer("confluentinc/cp-kafka:7.6.1")
                .withLogConsumer(outputFrame -> System.out.print(outputFrame.getUtf8String()));
    }
}
