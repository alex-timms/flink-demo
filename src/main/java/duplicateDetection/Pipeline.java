package duplicateDetection;

import duplicateDetection.common.records.DeadLetterRecord;
import duplicateDetection.object.Order;
import duplicateDetection.processor.DeserializeProcessor;
import duplicateDetection.processor.DuplicateDetector;
import duplicateDetection.processor.GapDetector;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.entity.Alert;

import static duplicateDetection.common.tags.Tags.DLQ_TAG;

public class Pipeline {

    private final KafkaSource<String> raw;
    private final StreamExecutionEnvironment env;
    private final Sink<Order> mainSink;
    private final Sink<DeadLetterRecord> dlqSink;
    private final Sink<Alert> alertSink;

    public Pipeline(KafkaSource<String> raw, StreamExecutionEnvironment env, Sink<Order> mainSink, Sink<DeadLetterRecord> dlqSink, Sink<Alert> alertSink){
        this.raw = raw;
        this.env = env;
        this.mainSink = mainSink;
        this.dlqSink = dlqSink;
        this.alertSink = alertSink;
    }

    public void run() {
        DataStream<String> rawtransactions = env
                .fromSource(
                        raw,
                        WatermarkStrategy.noWatermarks(),
                        "orders")
                .setParallelism(1);

        SingleOutputStreamOperator<Order> transactions = rawtransactions
                .process(new DeserializeProcessor())
                .name("deserializer");

        SingleOutputStreamOperator<Order> orders = transactions
                .keyBy(Order::getInstrumentId)
                .process(new GapDetector())
                .name("gap-detector");

        SingleOutputStreamOperator<Alert> alerts = orders
                .keyBy(Order::getInstrumentId)
                .process(new DuplicateDetector())
                .name("duplicate-detector");

        alerts.sinkTo(alertSink)
                .name("send-alerts");

        DataStream<DeadLetterRecord> dlqStream = alerts.getSideOutput(DLQ_TAG);
        dlqStream.sinkTo(dlqSink).name("DLQ-Sink");
        DataStream<DeadLetterRecord> gapStream = orders.getSideOutput(DLQ_TAG);
        gapStream.sinkTo(dlqSink).name("GAP-Sink");

    }
}
