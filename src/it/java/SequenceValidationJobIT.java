
import com.fasterxml.jackson.databind.ObjectMapper;
import duplicateDetection.Pipeline;
import duplicateDetection.common.enums.Side;
import duplicateDetection.common.records.DeadLetterRecord;
import duplicateDetection.common.records.MissingOrderId;
import duplicateDetection.config.ObjectMapperConfig;
import duplicateDetection.object.Order;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;
import sink.Sinks;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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

/**
 * End-to-end test: custom producer -> Kafka input topic -> Flink job -> Kafka output topic -> custom consumer.
 *
 * Adapt before use:
 *  - Order, GapRecord and OrderPipeline.build(...) are placeholders for your own classes.
 *  - Class names and the Kafka image depend on your Testcontainers version.
 *  - WriterInitContext is the Flink 1.19+ name (older versions use InitContext).
 *
 * Name the class *IT and run it with maven-failsafe-plugin so unit tests stay fast. Docker must be available.
 */
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
        kafkaConsumer.close();
        kafkaProducer.close();
        adminClient.close();
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
                .sorted(Comparator.comparingInt(order -> Math.toIntExact(order.getOrderId())))
                .toList();
        List<DeadLetterRecord> dlq = Sinks.CollectingSink.results(DLQ_TAG)
                .stream()
                .map(DeadLetterRecord.class::cast)
                .toList();
        assertThat(output)
                .usingRecursiveFieldByFieldElementComparator()
                .containsExactlyInAnyOrderElementsOf(List.of(order(1), order(2), order(2), order(4)));
        assertTrue(dlq.stream().allMatch(record -> "MissingOrderIdException".equals(record.errorMessage())));
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
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA.getBootstrapServers())
                .setTopics(inputTopic)
                .setGroupId("it-" + UUID.randomUUID())
                .setStartingOffsets(OffsetsInitializer.earliest())
                // Test only: makes the job finish once it has read what is already on the topic,
                // so env.execute() returns and the test is deterministic.
                .setBounded(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        // setup sink
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(KAFKA.getBootstrapServers())
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(outputTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();


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
