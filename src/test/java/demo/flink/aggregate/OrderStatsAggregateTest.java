package demo.flink.aggregate;

import demo.flink.common.enums.Side;
import demo.flink.object.Order;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class OrderStatsAggregateTest {

    // prices are scaled by 10,000: 100.00 is stored as 1_000_000, 102.00 as 1_020_000
    private static final long PRICE_100 = 1_000_000L;
    private static final long PRICE_102 = 1_020_000L;

    private final OrderStatsAggregate aggregate = new OrderStatsAggregate();

    private static Order order(long id, Side side, long price, long qty) {
        return new Order(id, 1, side, price, qty);
    }

    @Test
    public void testAdd_TwoBuyOrders_AccumulatesTotals() {
        // given
        var acc = aggregate.createAccumulator();

        // when
        aggregate.add(order(1, Side.BUY, PRICE_100, 10), acc);
        aggregate.add(order(2, Side.BUY, PRICE_102, 30), acc);
        var result = aggregate.getResult(acc);

        // then
        assertThat(result.count).isEqualTo(2);
        assertThat(result.totalQty).isEqualTo(40);
        assertThat(result.notional).isEqualTo(40_600_000L);   // 1_000_000 * 10 + 1_020_000 * 30
        assertThat(result.minPrice).isEqualTo(PRICE_100);
        assertThat(result.maxPrice).isEqualTo(PRICE_102);
        assertThat(result.buyQty).isEqualTo(40);
        assertThat(result.sellQty).isZero();
    }

    @Test
    public void testAdd_OneBuyOneSell_SplitsQuantityBySide() {
        // given
        var acc = aggregate.createAccumulator();

        // when
        aggregate.add(order(1, Side.BUY, PRICE_100, 10), acc);
        aggregate.add(order(2, Side.SELL, PRICE_102, 30), acc);
        var result = aggregate.getResult(acc);

        // then
        assertThat(result.count).isEqualTo(2);
        assertThat(result.totalQty).isEqualTo(40);            // both sides count towards total quantity
        assertThat(result.notional).isEqualTo(40_600_000L);
        assertThat(result.buyQty).isEqualTo(10);
        assertThat(result.sellQty).isEqualTo(30);
    }

    @Test
    public void testAdd_Throws_WhenTotalQuantityOverflows() {
        // given: price 1 keeps the notional (price * quantity) within range, so the quantity total is what overflows
        var acc = aggregate.createAccumulator();
        aggregate.add(order(1, Side.BUY, 1, Long.MAX_VALUE), acc);

        // when / then
        assertThatThrownBy(() -> aggregate.add(order(2, Side.BUY, 1, 1), acc))
                .isInstanceOf(ArithmeticException.class)
                .hasMessageContaining("overflow");
    }
}

