package demo.flink.aggregate;

import demo.flink.accumulator.OrderStatsAccumulator;
import demo.flink.object.OrderStats;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AttachWindowInfo
        extends ProcessWindowFunction<OrderStatsAccumulator, OrderStats, Integer, TimeWindow> {

    private static final double PRICE_SCALE = 10_000.0;

    @Override
    public void process(Integer instrumentId, Context context,
                        Iterable<OrderStatsAccumulator> results,
                        Collector<OrderStats> out) {
        OrderStatsAccumulator acc = results.iterator().next();   // exactly one: the pre-aggregated result
        TimeWindow window = context.window();
        double vwap = acc.totalQty == 0 ? 0 : acc.notional / (double) acc.totalQty / PRICE_SCALE;

        out.collect(new OrderStats(window.getStart(), window.getEnd(), instrumentId,
                acc.count, acc.totalQty, vwap, acc.minPrice, acc.maxPrice, acc.buyQty, acc.sellQty));
    }
}
