package demo.flink.object;

public record OrderStats(long windowStart,
                         long windowEnd,
                         int instrumentId,
                         long orderCount,
                         long totalQty,
                         double vwap,
                         long minPrice,
                         long maxPrice,
                         long buyQty,
                         long sellQty
)
{}
