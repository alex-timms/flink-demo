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

import com.fasterxml.jackson.databind.ObjectMapper;
import duplicateDetection.common.enums.Side;
import duplicateDetection.common.records.DeadLetterRecord;
import duplicateDetection.object.Order;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Skeleton code for implementing a fraud detector.
 */
public class DeserializeProcessor extends ProcessFunction<String, Order> {

//	private static final long serialVersionUID = 1L;
//
	private static final double SMALL_AMOUNT = 1.00;
	private static final double LARGE_AMOUNT = 500.00;
	private static final long ONE_MINUTE = 60 * 1000;

	private static final ObjectMapper objectMapper = new ObjectMapper();

	@Override
	public void open(OpenContext openContext) {
	}

	@Override
	public void processElement(
			String order,
			Context context,
			Collector<Order> collector) throws Exception {

		var deserializedOrder = objectMapper.readValue(order, Order.class);
		collector.collect(deserializedOrder);

	}
}
