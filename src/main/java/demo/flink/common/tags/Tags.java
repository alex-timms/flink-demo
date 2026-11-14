package demo.flink.common.tags;

import demo.flink.common.records.DeadLetterRecord;
import demo.flink.common.records.MissingOrderId;
import org.apache.flink.util.OutputTag;

public class Tags {
    public static final OutputTag<DeadLetterRecord> DLQ_TAG =
            new OutputTag<DeadLetterRecord>("dlq-output") {};

    public static final OutputTag<MissingOrderId> GAP_TAG =
            new OutputTag<MissingOrderId>("gap-output") {};
}
