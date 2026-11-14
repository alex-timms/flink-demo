package source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.util.UUID;

public class Sources {

    public static KafkaSource<String> stringKafkaSource(String bootstrap, String topic) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(topic)
                .setGroupId("it-" + UUID.randomUUID())
                .setStartingOffsets(OffsetsInitializer.earliest())
                // Test only: makes the job finish once it has read what is already on the topic,
                // so env.execute() returns and the test is deterministic.
                .setBounded(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }
}
