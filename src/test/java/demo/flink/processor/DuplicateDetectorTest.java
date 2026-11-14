package demo.flink.processor;

import demo.flink.common.enums.Side;
import demo.flink.common.records.DeadLetterRecord;
import demo.flink.object.Order;
import lombok.SneakyThrows;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentLinkedQueue;

import static demo.flink.common.tags.Tags.DLQ_TAG;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class DuplicateDetectorTest {


    @SneakyThrows
    @Test
    void test_processElement_NotDupe() {
        DuplicateDetector detector = new DuplicateDetector();

        // Automatically initializes RuntimeContext, state backend, and calls open()
        try (KeyedOneInputStreamOperatorTestHarness<Integer, Order, Order> testHarness =
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
            var initID = 1L;
            for (Order order : output) {
                assertThat(order.getOrderId()).isEqualTo(initID);
                initID++;
            }

        }
    }

    @SneakyThrows
    @Test
    void test_processElement_Dupe() {
        DuplicateDetector detector = new DuplicateDetector();
        Order order = new Order(1L, 1, Side.SELL, 10_000, 100);

        // Automatically initializes RuntimeContext, state backend, and calls open()
        try (KeyedOneInputStreamOperatorTestHarness<Integer, Order, Order> testHarness =
                     ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                             detector,
                             Order::getInstrumentId, // KeySelector
                             Types.INT   // Key TypeInformation
                     )) {

            // Send test elements with timestamps
            testHarness.processElement(order, 1000L);
            testHarness.processElement(order, 1000L);

            // Assert published outputs
            var output = testHarness.extractOutputValues();
            assertThat(output.size()).isEqualTo(1);
            var outputOrder = output.getFirst();
            assertThat(outputOrder.getOrderId()).isEqualTo(1);

            // Assert DLQ record
            ConcurrentLinkedQueue<StreamRecord<DeadLetterRecord>> dlqOutput = testHarness.getSideOutput(DLQ_TAG);

            assertNotNull(dlqOutput, "DLQ side output queue should not be null");
            assertEquals(1, dlqOutput.size(), "Exactly one record should be routed to DLQ");

            DeadLetterRecord dlqRecord = dlqOutput.poll().getValue();
            assertThat(dlqRecord.rawPayload()).isEqualTo(order.toString());
            assertThat(dlqRecord.errorMessage())
                    .isNotEmpty()
                    .isEqualTo("Duplicate order ID found");
            assertThat(dlqRecord.failureTimestamp()).isNotNull();
        }
    }

    @SneakyThrows
    @Test
    void test_processElement_InvalidOrderId() {
        DuplicateDetector detector = new DuplicateDetector();
        Order orderZero = new Order(0, 1, Side.SELL, 10_000, 100);
        Order orderOne = new Order(1L, 1, Side.SELL, 10_000, 100);

        // Automatically initializes RuntimeContext, state backend, and calls open()
        try (KeyedOneInputStreamOperatorTestHarness<Integer, Order, Order> testHarness =
                     ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                             detector,
                             Order::getInstrumentId, // KeySelector
                             Types.INT   // Key TypeInformation
                     )) {

            // Send test elements with timestamps
            testHarness.processElement(orderZero, 1000L);
            testHarness.processElement(orderOne, 1000L);

            // Assert outputs
            var output = testHarness.extractOutputValues();
            assertThat(output.size()).isEqualTo(1);
            var order = output.getFirst();
            assertThat(order.getOrderId()).isEqualTo(1);

            ConcurrentLinkedQueue<StreamRecord<DeadLetterRecord>> dlqOutput = testHarness.getSideOutput(DLQ_TAG);

            assertNotNull(dlqOutput, "DLQ side output queue should not be null");
            assertEquals(1, dlqOutput.size(), "Exactly one record should be routed to DLQ");

            DeadLetterRecord dlqRecord = dlqOutput.poll().getValue();
            assertThat(dlqRecord.rawPayload()).isEqualTo(orderZero.toString());
            assertThat(dlqRecord.errorMessage())
                    .isNotEmpty()
                    .isEqualTo("Invalid order ID found: 0");
            assertThat(dlqRecord.failureTimestamp()).isNotNull();
        }
    }
}
