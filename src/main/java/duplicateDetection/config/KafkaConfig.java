package duplicateDetection.config;

import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;

import static duplicateDetection.kafka.Sink.createKafkaSink;
import static duplicateDetection.kafka.Source.createKafkaSourceBuilder;

public class KafkaConfig {

    public static KafkaSource<String> kafkaSource() {
        return createKafkaSourceBuilder(
                ConfigLoader.get("broker"),
                ConfigLoader.get("inputTopic"),
                ConfigLoader.get("groupId")
        ).build();
    }

    public static KafkaSink<String> kafkaSink() {
        return createKafkaSink(
                ConfigLoader.get("broker"),
                ConfigLoader.get("outputTopic")
        );
    }
}
