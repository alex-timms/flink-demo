package demo.flink.config;

import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;

import static demo.flink.kafka.Sink.createKafkaSink;
import static demo.flink.kafka.Source.createKafkaSourceBuilder;

public class KafkaConfig {

    private KafkaConfig() {}

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
