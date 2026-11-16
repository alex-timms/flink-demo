package demo.flink.accumulator;


public class OrderStatsAccumulator {
    public long count;
    public long totalQty;
    public long notional;                    // sum of (scaled price * quantity)
    public long minPrice = Long.MAX_VALUE;
    public long maxPrice = Long.MIN_VALUE;
    public long buyQty;
    public long sellQty;

    public OrderStatsAccumulator () {}

}
