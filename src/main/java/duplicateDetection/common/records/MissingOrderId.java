package duplicateDetection.common.records;

public record MissingOrderId(long orderId,
                             int instrumentId)
{
    public String toString() {
        return  String.format(
                "Order: %d for instrument: %d is missing",
                orderId,
                instrumentId
        );
    }
}
