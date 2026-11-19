package demo.flink;

import demo.flink.aggregate.AttachWindowInfo;
import demo.flink.aggregate.OrderStatsAggregate;
import demo.flink.object.Order;
import demo.flink.object.OrderStats;
import demo.flink.processor.DeserializeProcessor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import java.time.Duration;


public class OrderStatsPipeline {

    private final Source<String, ?, ?> raw;
    private final StreamExecutionEnvironment env;
    private final Sink<OrderStats> mainSink;

    public OrderStatsPipeline(
            Source<String, ?, ?> raw,
            StreamExecutionEnvironment env,
            Sink<OrderStats> mainSink
    ) {
        this.raw = raw;
        this.env = env;
        this.mainSink = mainSink;
    }

    public void run() throws Exception {
        DataStream<String> rawOrders = env
                .fromSource(
                        raw,
                        WatermarkStrategy.forMonotonousTimestamps(),
                        "orders")
                .setParallelism(1);

        SingleOutputStreamOperator<Order> orders = rawOrders
                .process(new DeserializeProcessor())
                .name("deserializer")
                .setParallelism(1);


        SingleOutputStreamOperator<OrderStats> metrics = orders
                .keyBy(Order::getInstrumentId)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .aggregate(new OrderStatsAggregate(), new AttachWindowInfo());

        metrics.sinkTo(mainSink)
                .name("agg-sink");

        env.execute("aggregation-pipeline");
    }
}
