package demo.flink.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import demo.flink.common.enums.Side;
import demo.flink.common.records.MissingOrderId;
import demo.flink.config.ObjectMapperConfig;
import demo.flink.object.Order;
import lombok.SneakyThrows;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static demo.flink.common.tags.Tags.GAP_TAG;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class GapDetectorTest {

    ObjectMapper mapper = ObjectMapperConfig.objectMapper();

    @SneakyThrows
    @Test
    void test_processElement_NoGap() {
        GapDetector detector = new GapDetector();

        // Automatically initializes RuntimeContext, state backend, and calls open()
        try (KeyedOneInputStreamOperatorTestHarness<Integer, Order, String> testHarness =
                     ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                             detector,
                             Order::getInstrumentId, // KeySelector
                             Types.INT   // Key TypeInformation
                     )) {

            // Send test elements with timestamps
            testHarness.processElement(new Order(1L, 1, Side.SELL, 10_000, 100), 1000L);
            testHarness.processElement(new Order(2L, 1, Side.SELL, 11_000, 110), 1000L);
            testHarness.processElement(new Order(3L, 1, Side.SELL, 9_000, 120), 1000L);


            // Assert outputs
            var output = testHarness.extractOutputValues();

            assertThat(output.size()).isEqualTo(3);
            var orderID = 1L;
            for (String order : output) {
                assertThat(mapper.readValue(order, Order.class).getOrderId()).isEqualTo(orderID);
                orderID++;
            }

        }
    }

    @SneakyThrows
    @Test
    void test_processElement_SingleGap() {
        // given
        GapDetector detector = new GapDetector();
        var missingOrder = new MissingOrderId(2L, 1);

        // Automatically initializes RuntimeContext, state backend, and calls open()
        try (KeyedOneInputStreamOperatorTestHarness<Integer, Order, String> testHarness =
                     ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                             detector,
                             Order::getInstrumentId, // KeySelector
                             Types.INT   // Key TypeInformation
                     )) {

            // Send test elements with timestamps
            testHarness.processElement(new Order(1L, 1, Side.SELL, 10_000, 100), 1000L);
            testHarness.processElement(new Order(3L, 1, Side.SELL, 10_000, 100), 1000L);

            // Assert outputs
            var output = testHarness.extractOutputValues();
            assertThat(output.size()).isEqualTo(2);
            var orderOne = output.getFirst();
            assertThat(mapper.readValue(orderOne, Order.class).getOrderId()).isEqualTo(1);
            var orderTwo = output.getLast();
            assertThat(mapper.readValue(orderTwo, Order.class).getOrderId()).isEqualTo(3);

            ConcurrentLinkedQueue<StreamRecord<MissingOrderId>> gapOutput = testHarness.getSideOutput(GAP_TAG);

            assertNotNull(gapOutput, "GAP side output queue should not be null");
            assertEquals(1, gapOutput.size(), "Exactly one record should be routed to DLQ");

            MissingOrderId missingRecord = gapOutput.poll().getValue();
            assertThat(missingRecord.orderId()).isEqualTo(missingOrder.orderId());
            assertThat(missingRecord.instrumentId()).isEqualTo(missingOrder.instrumentId());
        }
    }

    @SneakyThrows
    @Test
    void test_processElement_BigGap() {
        // given
        GapDetector detector = new GapDetector();
        var missingOrder = new MissingOrderId(2L, 1);

        // Automatically initializes RuntimeContext, state backend, and calls open()
        try (KeyedOneInputStreamOperatorTestHarness<Integer, Order, String> testHarness =
                     ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                             detector,
                             Order::getInstrumentId, // KeySelector
                             Types.INT   // Key TypeInformation
                     )) {

            // Send test elements with timestamps
            testHarness.processElement(new Order(1L, 1, Side.SELL, 10_000, 100), 1000L);
            testHarness.processElement(new Order(5L, 1, Side.SELL, 10_000, 100), 1000L);

            // Assert outputs
            var output = testHarness.extractOutputValues();
            // assertEquals(...);
            assertThat(output.size()).isEqualTo(2);
            var orderOne = output.getFirst();
            assertThat(mapper.readValue(orderOne, Order.class).getOrderId()).isEqualTo(1);
            var orderTwo = output.getLast();
            assertThat(mapper.readValue(orderTwo, Order.class).getOrderId()).isEqualTo(5);

            ConcurrentLinkedQueue<StreamRecord<MissingOrderId>> gapOutput = testHarness.getSideOutput(GAP_TAG);

            assertNotNull(gapOutput, "GAP side output queue should not be null");
            assertEquals(3, gapOutput.size(), "Exactly one record should be routed to DLQ");

            List<MissingOrderId> missingOrderIdList = gapOutput
                    .stream()
                    .map(StreamRecord::getValue)
                    .toList();
            List<Long> missingIds = missingOrderIdList.stream()
                    .map(MissingOrderId::orderId)
                    .toList();
            List<Integer> missingInstruments = missingOrderIdList.stream()
                    .map(MissingOrderId::instrumentId)
                    .toList();
            assertThat(missingIds).isEqualTo(List.of(2L,3L,4L));
            assertThat(missingInstruments).isEqualTo(List.of(1,1,1));
        }
    }
}
