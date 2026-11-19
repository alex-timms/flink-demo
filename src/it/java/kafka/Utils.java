package kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import demo.flink.config.ObjectMapperConfig;
import demo.flink.object.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@Slf4j
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

    public static void produceOrders(KafkaProducer<String, String> kafkaProducer, String topic, List<Order> orders) throws Exception {
        for (Order order : orders) {
            kafkaProducer.send(new ProducerRecord<>(topic, MAPPER.writeValueAsString(order)));
        }
        kafkaProducer.flush();
    }

    public static void produceStrings(KafkaProducer<String, String> kafkaProducer, String topic, List<String> orders) throws Exception {
        List<Long> timestamps = List.of(
                1795035605000L,  // order 1, 21:00:05, window 1
                1795035635000L,  // order 2, 21:00:35, window 1
                1795035662000L,  // order 3, 21:01:02, window 2
                1795035675000L,  // order 4, 21:01:15, window 2
                1795035690000L,  // order 5, 21:01:30, window 2
                1795035705000L,  // order 6, 21:01:45, window 2
                1795035718000L   // order 7, 21:01:58, window 2
        );
        for (int i = 0; i < orders.size(); i++) {
            kafkaProducer.send(new ProducerRecord<>(topic, null, timestamps.get(i), null, orders.get(i)));
        }
        kafkaProducer.flush();
    }

    public static void createTopics(AdminClient admin, List<String> topics) {
        var topicConfig = topics.stream()
                .map(topic -> new NewTopic(topic, 1, (short) 1))
                .toList();
        admin.createTopics(topicConfig);
    }
}
