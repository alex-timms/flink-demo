package duplicateDetection.processor;

import duplicateDetection.common.records.DeadLetterRecord;
import duplicateDetection.object.Order;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;

import static duplicateDetection.common.tags.Tags.DLQ_TAG;

public class DuplicateDetector extends KeyedProcessFunction<Integer, Order, Alert> {

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
			Collector<Alert> collector) throws Exception {

		try {
			if (order.getOrderId() == 0) {
				throw new Exception("Order ID is 0");
			}
			// Get the current state for the current key
			Boolean seenTransaction = flagState.get(order.getOrderId());

			// Check if the flag is set
			if (seenTransaction != null) {
				if (seenTransaction) {
					// Output an alert downstream
					System.out.printf("Duplicate detected for order %d\n", order.getOrderId());
					Alert alert = new Alert();
					alert.setId(order.getOrderId());
					collector.collect(alert);
				}
			} else {
				System.out.println("Processing Order and adding new order to cache: " + order);
				flagState.put(order.getOrderId(), true);
				// Output an alert downstream
				Alert alert = new Alert();
				alert.setId(0L);
				collector.collect(alert);
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
