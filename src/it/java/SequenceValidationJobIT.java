import duplicateDetection.Pipeline;
import duplicateDetection.common.enums.Side;
import duplicateDetection.common.records.DeadLetterRecord;
import duplicateDetection.object.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import sink.Sinks;

import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import static kafka.Admin.createAdminClient;
import static kafka.Consumer.kafkaConsumer;
import static kafka.Container.createKafkaContainer;
import static kafka.Producer.kafkaProducer;
import static kafka.Utils.createTopics;
import static kafka.Utils.produceOrders;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static source.Sources.stringKafkaSource;

@Slf4j
@Testcontainers
class SequenceValidationJobIT {

    private static final String ORDERS_TAG = "ORDERS";
    private static final String ALERT_TAG = "ALERT";
    private static final String DLQ_TAG = "DLQ";

    @Container
    static final ConfluentKafkaContainer KAFKA = createKafkaContainer();

    private static KafkaConsumer<String, String> kafkaConsumer;
    private static KafkaProducer<String, String> kafkaProducer;
    private static AdminClient adminClient;

    private String inputTopic;
    private String outputTopic;

    @BeforeEach
    void setUp() throws Exception {
        String suffix = UUID.randomUUID().toString();
        inputTopic = "orders-in-" + suffix;
        outputTopic = "orders-out-" + suffix;

        kafkaConsumer = kafkaConsumer(KAFKA.getBootstrapServers());
        kafkaProducer = kafkaProducer(KAFKA.getBootstrapServers());
        adminClient = createAdminClient(KAFKA.getBootstrapServers());
        createTopics(adminClient, List.of(inputTopic, outputTopic));
        Sinks.CollectingSink.clear();
    }

    @AfterEach
    void shutDown() {
        Sinks.CollectingSink.clear();
        try {
            kafkaConsumer.close();
            kafkaProducer.close();
            adminClient.close();
        } catch (Exception e) {
            log.error("Error closing Kafka resources", e);
        }
    }

    @Test
    void removesDuplicatesAndReportsGaps() throws Exception {
        //given
        var orders = List.of(order(1), order(2), order(2), order(4));

        // when
        produceOrders(kafkaProducer, inputTopic, orders);
        runJob();

        // then
        List<Order> output = Sinks.CollectingSink.results(ORDERS_TAG)
                .stream()
                .map(Order.class::cast)
                .sorted(Comparator.comparingLong(Order::getOrderId))
                .toList();
        List<DeadLetterRecord> dlq = Sinks.CollectingSink.results(DLQ_TAG)
                .stream()
                .map(DeadLetterRecord.class::cast)
                .toList();
        assertThat(output)
                .usingRecursiveFieldByFieldElementComparator()
                .containsExactlyInAnyOrderElementsOf(List.of(order(1), order(2), order(2), order(4)));
        assertThat(dlq)
                .isNotEmpty()
                .allMatch(record -> "MissingOrderIdException".equals(record.errorMessage()));
        List<Alert> alerts = Sinks.CollectingSink.results(ALERT_TAG)
                .stream()
                .map(Alert.class::cast)
                .toList();
        assertEquals(4, alerts.size());
        assertEquals(1, alerts.stream().filter(alert -> alert.getId() == 2L).count());
    }

    // ---- Run the Flink job against the containerised Kafka ----

    private void runJob() throws Exception {
        // setup env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // setup source
        KafkaSource<String> source = stringKafkaSource(KAFKA.getBootstrapServers(), inputTopic);

        // setup Pipeline with every sink injected,
        var pipeline = new Pipeline(
                source,
                env,
                new Sinks.CollectingSink<Order>(ORDERS_TAG),
                new Sinks.CollectingSink<DeadLetterRecord>(DLQ_TAG),
                new Sinks.CollectingSink<Alert>(ALERT_TAG));

        pipeline.run();
    }

    private static Order order(long id) {
        return new Order(id, 1, Side.SELL, 10_000, 100); // placeholder constructor
    }
}
