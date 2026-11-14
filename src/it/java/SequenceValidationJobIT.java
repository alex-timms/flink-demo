import duplicateDetection.Pipeline;
import duplicateDetection.common.enums.Side;
import duplicateDetection.common.records.DeadLetterRecord;
import duplicateDetection.common.records.MissingOrderId;
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

import static duplicateDetection.common.tags.Tags.GAP_TAG;
import static kafka.Admin.createAdminClient;
import static kafka.Consumer.kafkaConsumer;
import static kafka.Container.createKafkaContainer;
import static kafka.Producer.kafkaProducer;
import static kafka.Utils.consumeKafkaOrders;
import static kafka.Utils.createTopics;
import static kafka.Utils.produceOrders;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static sink.Sinks.stringKafkaSink;
import static source.Sources.stringKafkaSource;

@Slf4j
@Testcontainers
class SequenceValidationJobIT {

    private static final String ORDERS_TAG = "ORDERS";
    private static final String GAP_TAG = "GAP";
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
        var expectedGap = new MissingOrderId(3, 1);
        var expectedDuplicate = new DeadLetterRecord(
                order(2).toString(),
                "Duplicate order ID found",
                System.currentTimeMillis()
        );
        // when
        produceOrders(kafkaProducer, inputTopic, orders);
        runJob();

        // then
        List<Order> output = consumeKafkaOrders(kafkaConsumer, 3, outputTopic)
                .stream()
                .sorted(Comparator.comparingLong(Order::getOrderId))
                .toList();
        List<DeadLetterRecord> dlq = Sinks.CollectingSink.results(DLQ_TAG)
                .stream()
                .map(DeadLetterRecord.class::cast)
                .toList();
        List<MissingOrderId> gaps = Sinks.CollectingSink.results(GAP_TAG)
                .stream()
                .map(MissingOrderId.class::cast)
                .toList();
        assertThat(output)
                .usingRecursiveFieldByFieldElementComparator()
                .containsExactlyInAnyOrderElementsOf(List.of(order(1), order(2), order(4)));
        assertThat(gaps)
                .isNotEmpty()
                .hasSize(1)
                .isEqualTo(List.of(expectedGap));
        assertThat(dlq)
                .hasSize(1);
        var dlqMessage =  dlq.getFirst();
        assertThat(dlqMessage.errorMessage())
                .isEqualTo(expectedDuplicate.errorMessage());
        assertThat(dlqMessage.rawPayload())
                .isEqualTo(expectedDuplicate.rawPayload());
    }

    // ---- Run the Flink job against the containerised Kafka ----

    private void runJob() throws Exception {
        // setup env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // setup source
        KafkaSource<String> source = stringKafkaSource(KAFKA.getBootstrapServers(), inputTopic);
        KafkaSink<String> sink = stringKafkaSink(KAFKA.getBootstrapServers(), outputTopic);

        // setup Pipeline with every sink injected,
        var pipeline = new Pipeline(
                source,
                env,
                sink,
                new Sinks.CollectingSink<DeadLetterRecord>(DLQ_TAG),
                new Sinks.CollectingSink<MissingOrderId>(GAP_TAG)
        );

        pipeline.run();
    }

    private static Order order(long id) {
        return new Order(id, 1, Side.SELL, 10_000, 100); // placeholder constructor
    }
}
