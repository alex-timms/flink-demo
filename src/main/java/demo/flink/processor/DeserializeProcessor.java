package demo.flink.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import demo.flink.object.Order;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class DeserializeProcessor extends ProcessFunction<String, Order> {

	private static final long serialVersionUID = 1L;

	private static final double SMALL_AMOUNT = 1.00;
	private static final double LARGE_AMOUNT = 500.00;
	private static final long ONE_MINUTE = 60 * 1000;

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	@Override
	public void open(OpenContext openContext) {
	}

	@Override
	public void processElement(
			String order,
			Context context,
			Collector<Order> collector) throws Exception {

		var deserializedOrder = OBJECT_MAPPER.readValue(order, Order.class);
		collector.collect(deserializedOrder);

	}
}
