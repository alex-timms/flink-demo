package duplicateDetection.common.tags;

import duplicateDetection.common.records.DeadLetterRecord;
import org.apache.flink.util.OutputTag;

public class Tags {
    public static final OutputTag<DeadLetterRecord> DLQ_TAG =
            new OutputTag<DeadLetterRecord>("dlq-output") {};
}
