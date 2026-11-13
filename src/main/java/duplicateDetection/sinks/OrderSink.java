package duplicateDetection.sinks;


import duplicateDetection.object.Order;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class OrderSink implements Sink<Order> {

    private static final Logger LOG = LoggerFactory.getLogger(OrderSink.class);

    @Override
    public SinkWriter<Order> createWriter(WriterInitContext writerInitContext) throws IOException {
        return new SinkWriter<Order>() {

            @Override
            public void write(Order order, Context context) throws IOException, InterruptedException {
                LOG.info(order.toString());
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
