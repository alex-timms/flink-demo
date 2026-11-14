# Flink Order Stream Demo

A small, non-production Apache Flink project that validates a stream of orders. It detects gaps in order numbers and duplicate order IDs, and routes each to its own sink. It is being built step by step to practise stateful stream processing, dead-letter handling and Kafka integration in Flink.

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
| Second job consuming the output feed and aggregating    | Planned |
| Increase complexity of deduplication using state        | Planned |

## Current functionality

The job reads orders from a custom `DataGeneratorSource` that deliberately produces a series of messages containing duplicate order IDs and gaps in order numbers. Two processors then check the stream.

```text
DataGeneratorSource (orders, including duplicates and gaps)
        |
        v
Processor 1: gap detection
        |-- gap found --> DLQ message (instrument + missing order ID) --> custom DLQ sink (logger)
        v
Processor 2: duplicate detection
        |-- duplicate found --> Alert (with the duplicated OrderID) --> custom alert sink / writer
```

| Component | What it does |
|---|---|
| Data generator source | Emits a fixed series of orders, including duplicates and missing order numbers, so the processors can be exercised repeatably. |
| Processor 1 (gaps) | Detects gaps in order numbers and creates a DLQ message, a custom object stating the instrument and the order ID found to be missing. |
| Processor 2 (duplicates) | Flags duplicate orders by sending an alert containing the duplicated OrderID. |
| DLQ sink | A custom-built logger that writes the DLQ messages. |
| Alert sink | A custom sink and writer for the duplicate alerts. |

## Target: next step

Replace the generator with Kafka for input, and write the processed output to Kafka instead of only raising alerts. Because orders will arrive from Kafka as strings, deserialisation becomes its own processor ahead of the gap-detection processor.

```text
Kafka input topic (string messages)
        |
        v
KafkaSource (raw strings)
        |
        v
Deserialisation processor: ObjectMapper, String --> Order
        |
        v
Processor 1: gap detection
        |-- gap found --> DLQ message (instrument + missing order ID) --> DLQ sink
        v
Processor 2: duplicate detection
        |-- duplicate --> alert, and removed from the stream
        v
Kafka output topic (deduplicated orders)
```

Intended result:
- The output topic contains each order once (deduplicated).
- Gaps in order numbers are identified and sent to the DLQ sink as messages naming the instrument and the missing order ID.
- Duplicates still produce an alert and do not appear in the output topic.

### Integration test (planned)

Build an integration test around this pipeline: publish a known series of orders, including duplicates and gaps, to the input topic, then check that:
- the output topic contains only deduplicated orders,
- each gap appears on the DLQ sink,
- each duplicate triggers an alert.

## Later: aggregation job

Once the output topic is the clean feed, a second Flink job will consume from it and aggregate the orders.

```text
Job 1 (this project): input topic --> deserialise --> gap + duplicate checks --> output topic
Job 2 (planned):      output topic --> aggregation
```

## Assumptions and limitations

- **Single instrument.** The pipeline is currently built for only one instrument.
- **Orders arrive in order.** Gap detection assumes that order numbers are meant to arrive in sequence, so out-of-order arrivals are not handled.
- **Basic detection logic, for now.** The gap and duplicate checks are deliberately very simple while the general project structure is built out. The aim is to increase the complexity of identifying gaps and duplicates over time by using more complex state combinations.

## Design notes and simplifications

- **String messages for now.** Messages are plain strings over Kafka, deserialised to the `Order` object with Jackson's `ObjectMapper`. This keeps the Kafka integration simple while the pipeline logic is built.
- **Deserialisation as a processor.** Parsing the incoming strings is a separate processing step ahead of the gap and duplicate checks, so each stage has a single responsibility.
- **Custom sinks.** The DLQ output is currently a custom logger, and the alerts use a custom sink and writer, so the processors can be run without external dependencies.
- **Demo scope.** This is a learning project, not a production system.

## Build and run

<!-- Add prerequisites (Java and Flink versions), build command, and how to run the job and tests. -->
