package duplicateDetection.sinks;

import duplicateDetection.common.records.DeadLetterRecord;
import duplicateDetection.object.Order;
import duplicateDetection.sinks.writers.LoggingSinkWriter;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import java.io.IOException;

public class DLQSink implements Sink<DeadLetterRecord> {
    @Override
    public SinkWriter<DeadLetterRecord> createWriter(WriterInitContext writerInitContext) throws IOException {
        return new LoggingSinkWriter();
    }
}
