package duplicateDetection.sinks;


import duplicateDetection.common.records.MissingOrderId;
import duplicateDetection.object.Order;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MissingOrderIdSink implements Sink<MissingOrderId> {

    private static final Logger LOG = LoggerFactory.getLogger(MissingOrderIdSink.class);

    @Override
    public SinkWriter<MissingOrderId> createWriter(WriterInitContext writerInitContext) throws IOException {
        return new SinkWriter<MissingOrderId>() {

            @Override
            public void write(MissingOrderId missingOrderId, Context context) throws IOException, InterruptedException {
                LOG.info(missingOrderId.toString());
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
