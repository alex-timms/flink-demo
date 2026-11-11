package duplicateDetection;

import duplicateDetection.common.enums.Side;
import duplicateDetection.common.records.DeadLetterRecord;
import duplicateDetection.object.Order;
import lombok.SneakyThrows;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentLinkedQueue;

import static duplicateDetection.common.tags.Tags.DLQ_TAG;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

public class DuplicateDetectorTest {


    @SneakyThrows
    @Test
    void test_processElement_NotDupe() {
        DuplicateDetector detector = new DuplicateDetector();

        // Automatically initializes RuntimeContext, state backend, and calls open()
        try (KeyedOneInputStreamOperatorTestHarness<Integer, Order, Alert> testHarness =
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
            for (Alert alert : output) {
                assertThat(alert.getId()).isEqualTo(0);
            }

        }
    }

    @SneakyThrows
    @Test
    void test_processElement_Dupe() {
        DuplicateDetector detector = new DuplicateDetector();

        // Automatically initializes RuntimeContext, state backend, and calls open()
        try (KeyedOneInputStreamOperatorTestHarness<Integer, Order, Alert> testHarness =
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
            var alertOne = output.get(0);
            var alertTwo = output.get(1);
            assertThat(alertOne.getId()).isEqualTo(0);
            assertThat(alertTwo.getId()).isEqualTo(1);
        }
    }

    @SneakyThrows
    @Test
    void test_processElement_DLQ() {
        DuplicateDetector detector = new DuplicateDetector();

        // Automatically initializes RuntimeContext, state backend, and calls open()
        try (KeyedOneInputStreamOperatorTestHarness<Integer, Order, Alert> testHarness =
                     ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                             detector,
                             Order::getInstrumentId, // KeySelector
                             Types.INT   // Key TypeInformation
                     )) {

            // Send test elements with timestamps
            testHarness.processElement(new Order(0L, 1, Side.SELL, 10_000, 100), 1000L);
            testHarness.processElement(new Order(1L, 1, Side.SELL, 10_000, 100), 1000L);

            // Assert outputs
            var output = testHarness.extractOutputValues();
            // assertEquals(...);
            assertThat(output.size()).isEqualTo(1);
            var alertOne = output.getFirst();
            assertThat(alertOne.getId()).isEqualTo(0);

            ConcurrentLinkedQueue<StreamRecord<DeadLetterRecord>> dlqOutput = testHarness.getSideOutput(DLQ_TAG);

            assertNotNull(dlqOutput, "DLQ side output queue should not be null");
            assertEquals(1, dlqOutput.size(), "Exactly one record should be routed to DLQ");

            DeadLetterRecord dlqRecord = dlqOutput.poll().getValue();
            assertTrue(dlqRecord.rawPayload().contains("Order: 0"));
            assertNotNull(dlqRecord.errorMessage());
        }
    }
}
