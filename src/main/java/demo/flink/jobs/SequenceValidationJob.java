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

package demo.flink.jobs;

import demo.flink.Pipeline;
import demo.flink.config.KafkaConfig;
import demo.flink.sinks.DLQSink;
import demo.flink.sinks.MissingOrderIdSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
public class SequenceValidationJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        var kafkaSource = KafkaConfig.kafkaSource();
        var kafkaSink = KafkaConfig.kafkaSink();

        Pipeline pipeline = new Pipeline(kafkaSource, env, kafkaSink, new DLQSink(), new MissingOrderIdSink());
        pipeline.run();

    }


}
