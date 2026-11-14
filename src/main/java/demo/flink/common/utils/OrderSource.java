package demo.flink.common.utils;

import demo.flink.common.enums.Side;
import demo.flink.object.Order;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class OrderSource {
    private static final long INITIAL_TIMESTAMP = Timestamp.valueOf("2019-01-01 00:00:00").getTime();
    private static final long SIX_MINUTES = 360000L;
    private static final double RECORDS_PER_SECOND = (double)10.0F;
    private static final List<Order> ORDERS = Arrays.asList(
            new Order(1L, 1, Side.SELL, 10_000, 100),
            new Order(2L, 1, Side.SELL, 10_000, 100),
            new Order(3L, 1, Side.SELL, 10_000, 100),
            new Order(4L, 1, Side.SELL, 10_000, 100),
            new Order(5L, 1, Side.SELL, 10_000, 100),
            new Order(5L, 1, Side.SELL, 10_000, 100),
            new Order(6L, 1, Side.SELL, 10_000, 100),
            new Order(7L, 1, Side.SELL, 10_000, 100),
            new Order(8L, 1, Side.SELL, 10_000, 100),
            new Order(9L, 1, Side.SELL, 10_000, 100),
            new Order(10L, 1, Side.SELL, 10_000, 100),
            new Order(11L, 1, Side.SELL, 10_000, 100),
            new Order(12L, 1, Side.SELL, 10_000, 100),
            new Order(13L, 1, Side.SELL, 10_000, 100),
            new Order(14L, 1, Side.SELL, 10_000, 100),
            new Order(15L, 1, Side.SELL, 10_000, 100),
            new Order(15L, 1, Side.SELL, 10_000, 100),
            new Order(16L, 1, Side.SELL, 10_000, 100),
            new Order(17L, 1, Side.SELL, 10_000, 100),
            new Order(18L, 1, Side.SELL, 10_000, 100),
            new Order(19L, 1, Side.SELL, 10_000, 100)
    );

    private OrderSource() {
    }

    public static DataGeneratorSource<Order> unbounded() {
        return create(Long.MAX_VALUE);
    }

    public static DataGeneratorSource<Order> bounded(long count) {
        return create(count);
    }

    private static DataGeneratorSource<Order> create(long count) {
        GeneratorFunction<Long, Order> generatorFunction = index -> ORDERS.get(index.intValue());
        return new DataGeneratorSource<>(
                generatorFunction,
                ORDERS.size(),
                RateLimiterStrategy.perSecond(10), // Optional: control emission speed
                TypeInformation.of(Order.class)         // Replace with your TypeInformation
        );    }
}
