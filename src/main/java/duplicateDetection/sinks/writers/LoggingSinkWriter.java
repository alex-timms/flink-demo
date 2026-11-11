package duplicateDetection.sinks.writers;

import duplicateDetection.common.records.DeadLetterRecord;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class LoggingSinkWriter implements SinkWriter<DeadLetterRecord> {

    private static final Logger LOG = LoggerFactory.getLogger(LoggingSinkWriter.class);

    @Override
    public void write(DeadLetterRecord deadLetterRecord, Context context) throws IOException, InterruptedException {
        LOG.error(
                "DLQ Record - Payload: {}, Error: {}, Timestamp: {}",
                deadLetterRecord.rawPayload(),
                deadLetterRecord.errorMessage(),
                deadLetterRecord.failureTimestamp()
        );
    }

    @Override
    public void flush(boolean b) throws IOException, InterruptedException {

    }

    @Override
    public void close() throws Exception {

    }
}
