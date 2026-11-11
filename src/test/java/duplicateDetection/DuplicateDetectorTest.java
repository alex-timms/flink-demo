package duplicateDetection;

import duplicateDetection.common.enums.Side;
import duplicateDetection.object.Order;
import lombok.SneakyThrows;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
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

            // Assert outputs
            var output = testHarness.extractOutputValues();
            // assertEquals(...);
            assertThat(output.size()).isEqualTo(1);
            var alert = output.get(0);
            assertThat(alert.getId()).isEqualTo(0);
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
}
