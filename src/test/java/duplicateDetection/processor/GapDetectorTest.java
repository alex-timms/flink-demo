package duplicateDetection.processor;

import duplicateDetection.common.enums.Side;
import duplicateDetection.common.records.DeadLetterRecord;
import duplicateDetection.common.records.MissingOrderId;
import duplicateDetection.object.Order;
import lombok.SneakyThrows;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentLinkedQueue;

import static duplicateDetection.common.tags.Tags.DLQ_TAG;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GapDetectorTest {


    @SneakyThrows
    @Test
    void test_processElement_NoGap() {
        GapDetector detector = new GapDetector();

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
            var orderID = 1L;
            for (Order order : output) {
                assertThat(order.getOrderId()).isEqualTo(orderID);
                orderID++;
            }

        }
    }

    @SneakyThrows
    @Test
    void test_processElement_Gap() {
        // given
        GapDetector detector = new GapDetector();
        var missingOrder = new MissingOrderId(2L, 1);

        // Automatically initializes RuntimeContext, state backend, and calls open()
        try (KeyedOneInputStreamOperatorTestHarness<Integer, Order, Order> testHarness =
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
            // assertEquals(...);
            assertThat(output.size()).isEqualTo(2);
            var orderOne = output.getFirst();
            assertThat(orderOne.getOrderId()).isEqualTo(1);
            var orderTwo = output.getLast();
            assertThat(orderTwo.getOrderId()).isEqualTo(3);

            ConcurrentLinkedQueue<StreamRecord<DeadLetterRecord>> dlqOutput = testHarness.getSideOutput(DLQ_TAG);

            assertNotNull(dlqOutput, "DLQ side output queue should not be null");
            assertEquals(1, dlqOutput.size(), "Exactly one record should be routed to DLQ");

            DeadLetterRecord dlqRecord = dlqOutput.poll().getValue();
            assertThat(dlqRecord.rawPayload()).isEqualTo(missingOrder.toString());
            assertThat(dlqRecord.errorMessage()).isNotBlank().isEqualTo("MissingOrderIdException");
        }
    }

    @SneakyThrows
    @Test
    void test_processElement_dupe() {
        // given
        GapDetector detector = new GapDetector();
        var missingOrder = new MissingOrderId(2L, 1);

        // Automatically initializes RuntimeContext, state backend, and calls open()
        try (KeyedOneInputStreamOperatorTestHarness<Integer, Order, Order> testHarness =
                     ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                             detector,
                             Order::getInstrumentId, // KeySelector
                             Types.INT   // Key TypeInformation
                     )) {

            // Send test elements with timestamps
            testHarness.processElement(new Order(1L, 1, Side.SELL, 10_000, 100), 1000L);
            testHarness.processElement(new Order(1L, 1, Side.SELL, 10_000, 100), 1000L);

            // Assert outputs
            var output = testHarness.extractOutputValues();
            // assertEquals(...);
            assertThat(output.size()).isEqualTo(2);
            var orderOne = output.getFirst();
            assertThat(orderOne.getOrderId()).isEqualTo(1);
            var orderTwo = output.getLast();
            assertThat(orderTwo.getOrderId()).isEqualTo(1);
            System.out.println(orderOne.toString());

            ConcurrentLinkedQueue<StreamRecord<DeadLetterRecord>> dlqOutput = testHarness.getSideOutput(DLQ_TAG);

            assertNotNull(dlqOutput, "DLQ side output queue should not be null");
            assertEquals(1, dlqOutput.size(), "Exactly one record should be routed to DLQ");

            DeadLetterRecord dlqRecord = dlqOutput.poll().getValue();
            assertThat(dlqRecord.rawPayload()).isEqualTo(missingOrder.toString());
            assertThat(dlqRecord.errorMessage()).isNotBlank().isEqualTo("MissingOrderIdException");
        }
    }
}
