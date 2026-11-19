# Flink Order Stream Demo

A small, non-production Apache Flink project that validates a stream of orders read from Kafka. 
It removes duplicate order IDs, detects gaps in order numbers, and writes the clean orders to an output Kafka topic. 
It is being built step by step to practise stateful stream processing, dead-letter handling and Kafka integration in Flink.

## Status at a glance

| Area                                                    | Status  |
|---------------------------------------------------------|---------|
| Custom data generator source (with duplicates and gaps) | Done    |
| Gap-detection processor, with dead-letter (DLQ) output  | Done    |
| Duplicate-detection processor, with alert output        | Done    |
| Custom DLQ sink (logger) and alert sink and writers     | Done    |
| Kafka source (read orders from a topic)                 | Done    |
| Deserialisation processor (JSON string to `Order`)      | Done    |
| Integration test around the full pipeline               | Done    |
| Kafka sink (deduplicated orders to an output topic)     | Done |
| Restructure Job to deduplicate before identifying gaps  | Done |
| Second job consuming the output feed and aggregating    | Done |
| Increase complexity of deduplication using state        | Planned |
| Add deserialisation error handling                      | Planned |

## Current functionality

The job reads JSON order strings from a Kafka input topic, deserialises them, 
removes duplicate order IDs, checks the remaining orders for gaps in the order 
numbers, and writes the clean orders to a Kafka output topic. Duplicates and gaps 
are each routed to their own side output and sink.

```text
Kafka input topic (JSON strings)
        |
        v
KafkaSource
        |
        v
Deserialisation processor: ObjectMapper, String --> Order
        |
        v   keyed by instrument
Processor 1: duplicate detection
        |-- duplicate (or invalid order ID) --> DeadLetterRecord --> DLQ sink (logger)
        v
        v   keyed by instrument
Processor 2: gap detection
        |-- gap found --> MissingOrderId (one per missing ID) --> gap sink (logger)
        v
Kafka output topic (deduplicated orders)
```

| Component | What it does |
|---|---|
| Kafka source | Reads raw string messages from the input topic, starting from the earliest offset. |
| Deserialisation processor | Parses each JSON string into an `Order` using Jackson's `ObjectMapper`. |
| Processor 1 (duplicates) | Keyed by instrument. Remembers the order IDs it has seen in Flink keyed state. A repeated ID, or an ID of `0`, is not passed on and instead becomes a `DeadLetterRecord` (raw payload, error message, timestamp) on the DLQ side output. |
| Processor 2 (gaps) | Keyed by instrument. Remembers the last order ID it saw. If the next ID is not the previous plus one, it emits a `MissingOrderId` (order ID and instrument) for every ID skipped, on the gap side output. The order itself is still passed on. |
| Kafka sink | Writes the deduplicated orders, as JSON strings, to the output topic. Delivery is at-least-once. |
| DLQ sink | A custom logger sink that writes the dead-letter records. |
| Gap sink | A custom logger sink that writes the missing-order-ID records. |

Configuration (broker, topics, consumer group ID) is read from `config.properties` on the classpath by `ConfigLoader`.

## Testing

| Test | What it covers |
|---|---|
| Unit tests (`DuplicateDetectorTest`, `GapDetectorTest`, `DeserializeProcessorTest`) | Each processor in isolation. |
| `ConfigLoaderTest` | Properties are loaded from the test `config.properties`. |
| `SequenceValidationJobIT` | The whole pipeline against a real Kafka broker. |

The integration test uses Testcontainers to start a Kafka broker (`confluentinc/cp-kafka:7.6.1`). 
It publishes the orders 1, 2, 2, 4 to the input topic, runs the pipeline, and then checks that:
- the output topic contains orders 1, 2 and 4 (the duplicate is removed),
- the duplicate of order 2 reaches the DLQ sink with the message "Duplicate order ID found",
- the gap sink receives a single record for the missing order 3.

Details:
- The source is bounded in the test, so the job finishes once it has read the messages already on the topic.
- The DLQ and gap sinks are replaced by a collecting test sink, so the test can assert on what they receive.
- The test reads its topic names from the `config.properties` on the classpath. 
- The broker address comes from the container, not from the file.
- Integration tests are named `*IT` and run with the Maven Failsafe plugin. They need Docker.

## Later: aggregation job

Once the output topic is the clean feed, a second Flink job will consume from it and aggregate the orders.

```text
Job 1 (this project): input topic --> deserialise --> duplicate check --> gap check --> output topic
Job 2 (planned):      output topic --> aggregation
```

## Assumptions and limitations

- **Orders start at 1 and arrive in order.** Gap detection assumes order numbers run in sequence from 1 for each instrument, so out-of-order arrivals are not handled. A late, lower ID would also reset the "last seen" ID.
- **Tested with a single instrument.** Both processors are keyed by instrument, so several instruments should work independently, but the integration test only uses one.
- **Malformed messages fail the job.** The deserialisation processor does not catch parsing errors, so one invalid JSON message throws and fails the job. Routing these to the DLQ is a planned improvement.
- **Unbounded state.** The duplicate detector keeps every order ID it has seen, with no expiry, so state grows without limit.
- **At-least-once output.** The Kafka sink uses at-least-once delivery, so a restart can write some orders to the output topic more than once. Exactly-once is not yet configured.
- **Basic detection logic, for now.** The gap and duplicate checks are deliberately simple while the general project structure is built out. The aim is to increase their complexity over time by using more complex state combinations.

## Design notes and simplifications

- **String messages for now.** Messages are plain JSON strings over Kafka, deserialised to the `Order` object with Jackson's `ObjectMapper`. This keeps the Kafka integration simple while the pipeline logic is built.
- **Deserialisation as a processor.** Parsing the incoming strings is a separate processing step ahead of the duplicate and gap checks, so each stage has a single responsibility.
- **Deduplicate before checking for gaps.** Duplicates are removed first, so the gap detector only sees each order ID once and a repeated ID cannot be mistaken for a gap or an out-of-sequence order.
- **Side outputs for bad data.** Duplicates and gaps leave the main stream as side outputs, so the clean order stream is not interrupted.
- **Sinks are injected.** `Pipeline` takes its source and all sinks as constructor arguments. The real job passes the Kafka source and sink plus the logger sinks, and the integration test passes a bounded Kafka source, the real Kafka sink, and collecting sinks.
- **Custom logger sinks.** The DLQ and gap output are written to logs by custom sinks, so no extra infrastructure is needed.
- **Demo scope.** This is a learning project, not a production system.

## Build and run

Prerequisites: Java 21, Maven, and Docker (for the integration tests). The project uses Flink 2.3.0.

```text
mvn test       # unit tests
mvn verify     # unit tests, then integration tests (needs Docker running)
```

To run the job, start a Kafka broker, set `broker`, `inputTopic`, `outputTopic` and `groupId` in `src/main/resources/config.properties`, and run `duplicateDetection.jobs.SequenceValidationJob`. In IntelliJ, enable "Add dependencies with 'provided' scope to classpath" in the run configuration, because the Flink core dependencies are marked `provided`.