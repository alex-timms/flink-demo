package demo.flink.processor;

import demo.flink.common.records.DeadLetterRecord;
import demo.flink.exceptions.DuplicateOrderIdException;
import demo.flink.exceptions.InvalidOrderIdException;
import demo.flink.object.Order;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import static demo.flink.common.tags.Tags.DLQ_TAG;

public class DuplicateDetector extends KeyedProcessFunction<Integer, Order, Order> {

    private static final long serialVersionUID = 1L;

    private static final double SMALL_AMOUNT = 1.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000;

	private transient MapState<Long, Boolean> flagState;

	@Override
	public void open(OpenContext openContext) {
		MapStateDescriptor<Long, Boolean> flagDescriptor = new MapStateDescriptor<>("map", Long.class, Boolean.class);
		flagState = getRuntimeContext().getMapState(flagDescriptor);
	}

	@Override
	public void processElement(
			Order order,
			Context context,
			Collector<Order> collector) {

		try {
			if (order.getOrderId() == 0) {
				throw new InvalidOrderIdException("0");
			}
			// Get the current state for the current key
			Boolean seenTransaction = flagState.get(order.getOrderId());

			// Check if the flag is set
			if (seenTransaction != null) {
				if (seenTransaction) {
					System.out.printf("Duplicate detected for order %d\n", order.getOrderId());
					throw new DuplicateOrderIdException();
				}
			} else {
				flagState.put(order.getOrderId(), true);
				collector.collect(order);
			}
		} catch (Exception e) {
			DeadLetterRecord dlqRecord = new DeadLetterRecord(
					order.toString(),
					e.getMessage(),
					System.currentTimeMillis()
			);
			context.output(DLQ_TAG,  dlqRecord);
		}
	}
}
