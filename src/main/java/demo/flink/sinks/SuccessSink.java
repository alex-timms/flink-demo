package demo.flink.sinks;


import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SuccessSink implements Sink<Alert> {

    private static final Logger LOG = LoggerFactory.getLogger(SuccessSink.class);

    @Override
    public SinkWriter<Alert> createWriter(WriterInitContext writerInitContext) throws IOException {
        return new SinkWriter<Alert>() {

            @Override
            public void write(Alert alert, Context context) throws IOException, InterruptedException {
                LOG.info(alert.toString());
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
