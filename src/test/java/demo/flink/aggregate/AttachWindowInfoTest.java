package demo.flink.aggregate;

import demo.flink.accumulator.OrderStatsAccumulator;
import demo.flink.object.OrderStats;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction.Context;

import java.util.List;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AttachWindowInfoTest {

    Context context = Mockito.mock(Context.class);
    Collector<OrderStats> collector = Mockito.mock(Collector.class);
    TimeWindow window = Mockito.mock(TimeWindow.class);

    AttachWindowInfo  info = new AttachWindowInfo();

    @Test
    public void testAdd_TwoBuyOrders_AccumulatesTotals() {
        // given
        var instrumentId = 1;
        var orderId = 1;
        var firstAcc = new OrderStatsAccumulator();
        firstAcc.count = 2;
        firstAcc.totalQty = 40;
        firstAcc.notional =  40_600_000L;
        firstAcc.minPrice =  1_000_000L;
        firstAcc.maxPrice = 1_020_000L;
        firstAcc.buyQty =40;
        firstAcc.sellQty = 0;
        var currentTime = System.currentTimeMillis();
        var endTime = currentTime + 1000;


        when(context.window()).thenReturn(window);
        when(window.getStart()).thenReturn(currentTime);
        when(window.getEnd()).thenReturn(endTime);


        // when
        info.process(instrumentId, context, List.of(firstAcc), collector);

        // then
        verify(collector).collect(
                new OrderStats(
                        currentTime,
                        endTime,
                        instrumentId,
                        2,
                        40,
                        101.5,
                        1_000_000L,
                        1020000,
                        40,
                        0
                        )
        );
    }
}
