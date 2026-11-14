package duplicateDetection.common.tags;

import duplicateDetection.common.records.DeadLetterRecord;
import duplicateDetection.common.records.MissingOrderId;
import org.apache.flink.util.OutputTag;

public class Tags {
    public static final OutputTag<DeadLetterRecord> DLQ_TAG =
            new OutputTag<DeadLetterRecord>("dlq-output") {};

    public static final OutputTag<MissingOrderId> GAP_TAG =
            new OutputTag<MissingOrderId>("gap-output") {};
}
