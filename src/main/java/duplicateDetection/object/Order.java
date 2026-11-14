package duplicateDetection.object;

import duplicateDetection.common.enums.Side;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Order {
    public long orderId;
    public int instrumentId;
    public Side side;
    public long price;
    public long leavesQty;

    public Order(){}

    public Order(long orderId, int instrumentId, Side side, long price, long qty) {
        this.orderId = orderId;
        this.instrumentId = instrumentId;
        this.side = side;
        this.price = price;
        this.leavesQty = qty;
    }

    @Override
    public String toString() {
        return String.format(
                "{\"orderId\":%d,\"instrumentId\":%d,\"side\":\"%s\",\"price\":%d,\"leavesQty\":%d}",
                orderId,
                instrumentId,
                side,
                price,
                leavesQty
        );
    }
}
