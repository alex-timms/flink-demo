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
import duplicateDetection.common.utils.OrderSource;
import duplicateDetection.object.Order;
import duplicateDetection.sinks.SuccessSink;
import duplicateDetection.sinks.DLQSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.entity.Alert;


import static duplicateDetection.common.tags.Tags.DLQ_TAG;

/**
 * Skeleton code for the DataStream API walkthrough.
 *
 * <p>This job reads an infinite stream of credit card transactions, detects fraudulent
 * patterns, and outputs alerts.
 *
 * <p>To run this example in your IDE:
 * <ul>
 *   <li>IntelliJ IDEA: Go to Run > Edit Configurations > Modify options > Select
 *       "include dependencies with 'Provided' scope"</li>
 * </ul>
 */
public class DuplicateDetectionJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Order> transactions = env
            .fromSource(
                OrderSource.unbounded(),
                WatermarkStrategy.noWatermarks(),
                "orders");

        SingleOutputStreamOperator<Alert> alerts = transactions
            .keyBy(Order::getInstrumentId)
            .process(new DuplicateDetector())
            .name("duplicate-detector");

        alerts.sinkTo(new SuccessSink())
            .name("send-alerts");

        DataStream<DeadLetterRecord> dlqStream = alerts.getSideOutput(DLQ_TAG);
        dlqStream.sinkTo(new DLQSink()).name("DLQ-Sink");

        env.execute("Duplicate Detection");
    }
}
