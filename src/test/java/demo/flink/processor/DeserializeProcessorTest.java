package demo.flink.processor;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import demo.flink.common.enums.Side;
import demo.flink.config.ObjectMapperConfig;
import demo.flink.object.Order;
import lombok.SneakyThrows;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class DeserializeProcessorTest {

    @SneakyThrows
    @Test
    void test_processElement_NoGap() {
        DeserializeProcessor deserializer = new DeserializeProcessor();
        var order = new Order(1L, 1, Side.SELL, 10_000, 100);

        // Automatically initializes RuntimeContext, state backend, and calls open()
        try (OneInputStreamOperatorTestHarness<String, Order> testHarness =
                     ProcessFunctionTestHarnesses.forProcessFunction(
                             deserializer
                     )) {

            // Send test elements with timestamps
            testHarness.processElement("{\"orderId\":1,\"instrumentId\":1,\"side\":\"SELL\",\"price\":10000,\"leavesQty\":100}", 1000L);


            // Assert outputs
            var output = testHarness.extractOutputValues();

            assertThat(output.size()).isEqualTo(1);
            var result = output.getFirst();
            assertThat(result.getOrderId()).isEqualTo(order.getOrderId());
            assertThat(result.getInstrumentId()).isEqualTo(order.getInstrumentId());
            assertThat(result.getSide()).isEqualTo(order.getSide());
            assertThat(result.getPrice()).isEqualTo(order.getPrice());
            assertThat(result.getLeavesQty()).isEqualTo(order.getLeavesQty());

        }
    }

    @SneakyThrows
    @Test
    void test_processElement_NoGap2() {
        ObjectMapper objectMapper = ObjectMapperConfig.objectMapper();

        var order = new Order(1L, 1, Side.SELL, 10_000, 100);

        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        System.out.println(objectMapper.writeValueAsString(order));



    }
}
