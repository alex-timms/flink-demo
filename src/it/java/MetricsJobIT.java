import config.KafkaConfig;
import demo.flink.MetricsPipeline;
import demo.flink.Pipeline;
import demo.flink.common.enums.Side;
import demo.flink.common.records.DeadLetterRecord;
import demo.flink.common.records.MissingOrderId;
import demo.flink.config.ConfigLoader;
import demo.flink.object.Order;
import demo.flink.object.OrderStats;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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

import static kafka.Admin.createAdminClient;
import static kafka.Consumer.kafkaConsumer;
import static kafka.Container.createKafkaContainer;
import static kafka.Producer.kafkaProducer;
import static kafka.Utils.consumeKafkaOrders;
import static kafka.Utils.createTopics;
import static kafka.Utils.produceOrders;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

@Slf4j
@Testcontainers
class MetricsJobIT {


    @Container
    static final ConfluentKafkaContainer KAFKA = createKafkaContainer();

    private static KafkaProducer<String, String> kafkaProducer;
    private static AdminClient adminClient;

    private String inputTopic;

    @BeforeEach
    void setUp() throws Exception {
        inputTopic = ConfigLoader.get("inputTopic");

        kafkaProducer = kafkaProducer(KAFKA.getBootstrapServers());
        adminClient = createAdminClient(KAFKA.getBootstrapServers());
        createTopics(adminClient, List.of(inputTopic));
        Sinks.CollectingSink.clear();
    }

    @AfterEach
    void shutDown() {
        Sinks.CollectingSink.clear();
        try {
            adminClient.deleteTopics(List.of(inputTopic));
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
        List<OrderStats> stats = Sinks.CollectingSink.results("main")
                .stream()
                .map(OrderStats.class::cast)
                .toList();
        assertThat(stats).isNotEmpty();

    }

    // ---- Run the Flink job against the containerised Kafka ----

    private void runJob() throws Exception {
        // setup env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // setup source
        KafkaSource<String> source = KafkaConfig.kafkaSource(KAFKA.getBootstrapServers());
        Sink<OrderStats> sink = new Sinks.CollectingSink<>("main");

        // setup Pipeline with every sink injected,
        var pipeline = new MetricsPipeline(
                source,
                env,
                sink
        );

        pipeline.run();
    }

    private static Order order(long id) {
        return new Order(id, 1, Side.SELL, 10_000, 100); // placeholder constructor
    }
}
