package duplicateDetection.config;

import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;

import static duplicateDetection.kafka.Sink.createKafkaSink;
import static duplicateDetection.kafka.Source.createKafkaSource;

public class KafkaConfig {

    public static KafkaSource<String> kafkaSource() {
        return createKafkaSource("broker", "topic", "groupId");
    }

    public static KafkaSink<String> kafkaSink() {
        return createKafkaSink("broker", "topic");
    }
}
