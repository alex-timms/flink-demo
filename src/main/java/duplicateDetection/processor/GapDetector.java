/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package duplicateDetection.processor;

import duplicateDetection.common.records.DeadLetterRecord;
import duplicateDetection.common.records.MissingOrderId;
import duplicateDetection.object.Order;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import static duplicateDetection.common.tags.Tags.DLQ_TAG;

/**
 * Skeleton code for implementing a fraud detector.
 */
public class GapDetector extends KeyedProcessFunction<Integer, Order, Order> {

//	private static final long serialVersionUID = 1L;
//
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
			Collector<Order> collector) throws Exception {

			var previousID = lastIdValueState.value();
			if (previousID == null) {
				previousID = 0L;
			}
			var currentID = order.getOrderId();
			if (previousID + 1 != currentID) {
				MissingOrderId missingOrderId = new MissingOrderId(previousID + 1 , order.getInstrumentId());
				// Output an alert downstream
				DeadLetterRecord dlqRecord = new DeadLetterRecord(
						missingOrderId.toString(),
						"MissingOrderIdException",
						System.currentTimeMillis()
				);
				context.output(DLQ_TAG,  dlqRecord);
			}
			lastIdValueState.update(currentID);
			collector.collect(order);
		};

}
