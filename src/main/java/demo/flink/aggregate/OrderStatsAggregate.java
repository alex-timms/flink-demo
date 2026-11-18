package demo.flink.aggregate;

import demo.flink.accumulator.OrderStatsAccumulator;
import demo.flink.common.enums.Side;
import demo.flink.object.Order;
import org.apache.flink.api.common.functions.AggregateFunction;

public class OrderStatsAggregate implements AggregateFunction<Order, OrderStatsAccumulator, OrderStatsAccumulator> {
    @Override
    public OrderStatsAccumulator createAccumulator() {
        return new OrderStatsAccumulator();
    }

    @Override
    public OrderStatsAccumulator add(Order value, OrderStatsAccumulator accumulator) {
        long qty = value.getLeavesQty();
        accumulator.count++;
        accumulator.totalQty = Math.addExact(accumulator.totalQty, qty);
        accumulator.notional = Math.addExact(accumulator.notional, Math.multiplyExact(value.getPrice(), qty));
        accumulator.minPrice = Math.min(accumulator.minPrice, value.getPrice());
        accumulator.maxPrice = Math.max(accumulator.maxPrice, value.getPrice());
        if (value.getSide() == Side.BUY) {
            accumulator.buyQty += qty;
        } else {
            accumulator.sellQty += qty;
        }
        return accumulator;                                             // updated in place
    }

    @Override
    public OrderStatsAccumulator getResult(OrderStatsAccumulator accumulator) {
        return accumulator;
    }

    @Override
    public OrderStatsAccumulator merge(OrderStatsAccumulator a, OrderStatsAccumulator b) {
        a.count += b.count;
        a.totalQty += b.totalQty;
        a.notional += b.notional;
        a.minPrice = Math.min(a.minPrice, b.minPrice);
        a.maxPrice = Math.max(a.maxPrice, b.maxPrice);
        a.buyQty += b.buyQty;
        a.sellQty += b.sellQty;
        return a;
    }
}
