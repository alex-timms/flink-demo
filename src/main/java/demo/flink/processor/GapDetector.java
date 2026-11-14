package demo.flink.processor;

import demo.flink.common.records.MissingOrderId;
import demo.flink.object.Order;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import static demo.flink.common.tags.Tags.GAP_TAG;

public class GapDetector extends KeyedProcessFunction<Integer, Order, String> {

	private static final long serialVersionUID = 1L;

	private static final double SMALL_AMOUNT = 1.00;
	private static final double LARGE_AMOUNT = 500.00;
	private static final long ONE_MINUTE = 60 * 1000;

	private transient ValueState<Long> lastIdValueState;

	@Override
	public void open(OpenContext openContext) {
		ValueStateDescriptor<Long> flagDescriptor = new ValueStateDescriptor<>("previousID", Long.class);
		lastIdValueState = getRuntimeContext().getState(flagDescriptor);
	}

	@Override
	public void processElement(
			Order order,
			Context context,
			Collector<String> collector) throws Exception {

			var previousID = lastIdValueState.value();
			if (previousID == null) {
				previousID = 0L;
			}
			var currentID = order.getOrderId();
			if (previousID + 1 != currentID) {
				for (int i = Math.toIntExact(previousID + 1); i < currentID; i++) {
					// Output an alert downstream
					MissingOrderId missingRecord = new MissingOrderId(
							i,
							order.getInstrumentId()
					);
					context.output(GAP_TAG,  missingRecord);
				}
			}
			lastIdValueState.update(currentID);
			collector.collect(order.toString());
		};

}
