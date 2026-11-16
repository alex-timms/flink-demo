package demo.flink;

import demo.flink.accumulator.OrderStatsAccumulator;
import demo.flink.aggregate.OrderStatsAggregate;
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
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import java.time.Duration;

import static demo.flink.common.tags.Tags.DLQ_TAG;
import static demo.flink.common.tags.Tags.GAP_TAG;

public class MetricsPipeline {

    private final Source<String, ?, ?> raw;
    private final StreamExecutionEnvironment env;
    private final Sink<String> mainSink;
    private final Sink<DeadLetterRecord> dlqSink;
    private final Sink<MissingOrderId> gapSink;

    public MetricsPipeline(
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
        DataStream<String> rawOrders = env
                .fromSource(
                        raw,
                        WatermarkStrategy.noWatermarks(),
                        "orders")
                .setParallelism(1);

        SingleOutputStreamOperator<Order> orders = rawOrders
                .process(new DeserializeProcessor())
                .name("deserializer")
                .setParallelism(1);


        SingleOutputStreamOperator<OrderStatsAccumulator> metrics = orders
                .keyBy(Order::getInstrumentId)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
                .aggregate(new OrderStatsAggregate());

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
