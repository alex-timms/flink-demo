package demo.flink.aggregate;

import demo.flink.accumulator.OrderStatsAccumulator;
import demo.flink.object.Order;
import org.apache.flink.api.common.functions.AggregateFunction;

public class OrderStatsAggregate implements AggregateFunction<Order, OrderStatsAccumulator, OrderStatsAccumulator> {
    @Override
    public OrderStatsAccumulator createAccumulator() {
        return new OrderStatsAccumulator();
    }

    @Override
    public OrderStatsAccumulator add(Order value, OrderStatsAccumulator accumulator) {
        accumulator.buyQty =+ value.leavesQty;
        return accumulator;
    }

    @Override
    public OrderStatsAccumulator getResult(OrderStatsAccumulator accumulator) {
        return null;
    }

    @Override
    public OrderStatsAccumulator merge(OrderStatsAccumulator a, OrderStatsAccumulator b) {
        return null;
    }
}
