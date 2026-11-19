package demo.flink.sinks;


import demo.flink.object.OrderStats;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class OrderStatsSink implements Sink<OrderStats> {

    private static final Logger LOG = LoggerFactory.getLogger(OrderStatsSink.class);

    @Override
    public SinkWriter<OrderStats> createWriter(WriterInitContext writerInitContext) throws IOException {
        return new SinkWriter<OrderStats>() {

            @Override
            public void write(OrderStats orderStats, Context context) throws IOException, InterruptedException {
                LOG.info(orderStats.toString());
            }

            @Override
            public void flush(boolean b) throws IOException, InterruptedException {

            }

            @Override
            public void close() throws Exception {

            }
        };
    }
}
