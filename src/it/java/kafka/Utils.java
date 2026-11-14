package kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import duplicateDetection.config.ObjectMapperConfig;
import duplicateDetection.object.Order;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class Utils {

    private static final ObjectMapper MAPPER = ObjectMapperConfig.objectMapper();

    public static List<Order> consumeKafkaOrders(KafkaConsumer<String, String> kafkaConsumer, int expected, String outputTopic) throws Exception {
        return consumeKafkaOrders(kafkaConsumer, expected, Duration.ofSeconds(3), outputTopic);
    }
    public static List<Order> consumeKafkaOrders(KafkaConsumer<String, String> kafkaConsumer, int expected, Duration timeout, String outputTopic) throws Exception {
        List<Order> received = new ArrayList<>();
        long deadline = System.nanoTime() + timeout.toNanos();
        kafkaConsumer.subscribe(List.of(outputTopic));

        while (received.size() < expected && System.nanoTime() < deadline) {
            for (ConsumerRecord<String, String> record : kafkaConsumer.poll(Duration.ofMillis(500))) {
                received.add(MAPPER.readValue(record.value(), Order.class));
            }
        }
        // Brief extra poll so a stray extra message (for example a leaked duplicate) fails the assertion.
        for (ConsumerRecord<String, String> record : kafkaConsumer.poll(Duration.ofSeconds(2))) {
            received.add(MAPPER.readValue(record.value(), Order.class));
        }

        return received;
    }
}
