
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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;
import sink.Sinks;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static kafka.Consumer.kafkaConsumer;
import static kafka.Producer.kafkaProducer;
import static org.junit.jupiter.api.Assertions.assertEquals;

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

    @Container
    static final ConfluentKafkaContainer KAFKA =
            new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))
                    .withLogConsumer(outputFrame -> System.out.print(outputFrame.getUtf8String()))
                    .withStartupTimeout(Duration.ofMinutes(2));

    private static final ObjectMapper MAPPER = ObjectMapperConfig.objectMapper();
    private static KafkaConsumer<String, String> kafkaConsumer;
    private static KafkaProducer<String, String> kafkaProducer;

    private String inputTopic;
    private String outputTopic;

    @BeforeEach
    void setUp() throws Exception {
        // Fresh topics per test, so tests never see each other's messages.
        String suffix = UUID.randomUUID().toString();
        inputTopic = "orders-in-" + suffix;
        outputTopic = "orders-out-" + suffix;

        try (AdminClient admin = AdminClient.create(
                Map.<String, Object>of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            // One partition keeps ordering simple for assertions.
            admin.createTopics(List.of(
                    new NewTopic(inputTopic, 1, (short) 1),
                    new NewTopic(outputTopic, 1, (short) 1))).all().get();
        }
        kafkaConsumer = kafkaConsumer(KAFKA.getBootstrapServers());
        kafkaProducer = kafkaProducer(KAFKA.getBootstrapServers());
        Sinks.CollectingSink.clear();
    }

    @Test
    void removesDuplicatesAndReportsGaps() throws Exception {
        //given
        produce(List.of(order(1), order(2), order(2), order(4)));

        // when
        runJob();

        // then
        List<Order> output = consumeOrders(3, Duration.ofSeconds(30));
        assertEquals(List.of(order(1), order(2), order(4)), output);
        assertEquals(List.of(new MissingOrderId(2L, 1)), Sinks.CollectingSink.results("dlq"));
        assertEquals(1, Sinks.CollectingSink.results("alerts").size());
    }

    // ---- Custom producer: writes JSON strings to the job's source topic ----

    private void produce(List<Order> orders) throws Exception {
            for (Order order : orders) {
                kafkaProducer.send(new ProducerRecord<>(inputTopic, MAPPER.writeValueAsString(order)));
            }
            kafkaProducer.flush();
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
        var pipeline = new Pipeline(source, env, new Sinks.CollectingSink<Order>("Orders"), new Sinks.CollectingSink<DeadLetterRecord>("DLQ"), new Sinks.CollectingSink<Alert>("Alert"));
        pipeline.run();

    }

    // ---- Custom consumer: reads what the job wrote to the sink topic ----

    private List<Order> consumeOrders(int expected, Duration timeout) throws Exception {
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


    private static Order order(long id) {
        return new Order(id, 1, Side.SELL, 10_000, 100); // placeholder constructor
    }
}
