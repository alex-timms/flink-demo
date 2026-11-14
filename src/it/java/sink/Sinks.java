package sink;

import duplicateDetection.object.Order;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;


public class Sinks {


    /**
     * Stores everything it receives in a static map, so the test can assert on it.
     * Works because Flink's local mini cluster runs in the same JVM as the test.
     * With parallelism above 1, arrival order is not guaranteed.
     */
    public static class CollectingSink<T> implements Sink<T> {
        private static final Map<String, List<Object>> STORE = new ConcurrentHashMap<>();
        private final String name;

        public CollectingSink(String name) {
            this.name = name;
        }

        public static List<Object> results(String name) {
            return STORE.getOrDefault(name, List.of());
        }

        public static void clear() {
            STORE.clear();
        }

        @Override
        public SinkWriter<T> createWriter(WriterInitContext context) {
            return new SinkWriter<T>() {
                @Override
                public void write(T element, Context ctx) {
                    STORE.computeIfAbsent(name, k -> new CopyOnWriteArrayList<>()).add(element);
                }

                @Override
                public void flush(boolean endOfInput) {
                }

                @Override
                public void close() {
                }
            };
        }
    }

    public static KafkaSink<String> stringKafkaSink(String bootstrap, String topic) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(bootstrap)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }

}
