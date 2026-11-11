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

package duplicateDetection;

import duplicateDetection.common.records.DeadLetterRecord;
import duplicateDetection.object.Order;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.function.TriConsumer;
import org.apache.flink.walkthrough.common.entity.Alert;

import java.util.function.Function;

import static duplicateDetection.common.tags.Tags.DLQ_TAG;

/**
 * Skeleton code for implementing a fraud detector.
 */
public class DuplicateDetector extends KeyedProcessFunction<Integer, Order, Alert> {

//	private static final long serialVersionUID = 1L;
//
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
			System.out.printf("Duplicate detector received order: %s\n", order);

			// Check if the flag is set
			if (seenTransaction != null) {
				if (seenTransaction) {
					// Output an alert downstream
					Alert alert = new Alert();
					alert.setId(order.getOrderId());
					collector.collect(alert);
				}
			} else {
				System.out.println("Adding new order to cache: " + order);
				flagState.put(order.getOrderId(), true);
				// Output an alert downstream
				Alert alert = new Alert();
				alert.setId(0);
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
