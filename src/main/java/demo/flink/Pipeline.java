package demo.flink;

import demo.flink.common.records.DeadLetterRecord;
import demo.flink.common.records.MissingOrderId;
import demo.flink.object.Order;
import demo.flink.processor.DeserializeProcessor;
import demo.flink.processor.DuplicateDetector;
import demo.flink.processor.GapDetector;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static demo.flink.common.tags.Tags.DLQ_TAG;
import static demo.flink.common.tags.Tags.GAP_TAG;

public class Pipeline {

    private final Source<String, ?, ?> raw;
    private final StreamExecutionEnvironment env;
    private final Sink<String> mainSink;
    private final Sink<DeadLetterRecord> dlqSink;
    private final Sink<MissingOrderId> gapSink;

    public Pipeline(
            Source<String, ?, ?> raw,
            StreamExecutionEnvironment env,
            Sink<String> mainSink,
            Sink<DeadLetterRecord> dlqSink,
            Sink<MissingOrderId> gapSink
    ) {
        this.raw = raw;
        this.env = env;
        this.mainSink = mainSink;
        this.dlqSink = dlqSink;
        this.gapSink = gapSink;
    }

    public void run() throws Exception {
        DataStream<String> rawtransactions = env
                .fromSource(
                        raw,
                        WatermarkStrategy.noWatermarks(),
                        "orders")
                .setParallelism(1);

        SingleOutputStreamOperator<Order> transactions = rawtransactions
                .process(new DeserializeProcessor())
                .name("deserializer")
                .setParallelism(1);

        SingleOutputStreamOperator<Order> dedupeTransactions = transactions
                .keyBy(Order::getInstrumentId)
                .process(new DuplicateDetector())
                .name("duplicate-detector");

        SingleOutputStreamOperator<String> orders = dedupeTransactions
                .keyBy(Order::getInstrumentId)
                .process(new GapDetector())
                .name("gap-detector");

        orders.sinkTo(mainSink)
                .name("order-sink");

        DataStream<DeadLetterRecord> dlqStream =
                dedupeTransactions.getSideOutput(DLQ_TAG);
        dlqStream.sinkTo(dlqSink).name("DLQ-Sink");

        DataStream<MissingOrderId> gapStream =
                orders.getSideOutput(GAP_TAG);
        gapStream.sinkTo(gapSink).name("GAP-Sink");

        env.execute("sequence-validation-pipeline");
    }
}
