package config;

import duplicateDetection.config.ConfigLoader;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import static duplicateDetection.kafka.Sink.createKafkaSink;
import static duplicateDetection.kafka.Source.createKafkaSourceBuilder;

public class KafkaConfig {

    public static KafkaSource<String> kafkaSource(String bootstrapServer) {
        return createKafkaSourceBuilder(
                    bootstrapServer,
                    ConfigLoader.get("inputTopic"),
                    ConfigLoader.get("groupId")
                )
                .setBounded(OffsetsInitializer.latest())
                .build();
    }

    public static KafkaSink<String> kafkaSink(String bootstrapServer) {
        return createKafkaSink(
                bootstrapServer,
                ConfigLoader.get("outputTopic")
        );
    }
}
