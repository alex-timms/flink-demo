package demo.flink.common.records;

import java.io.Serializable;

public record DeadLetterRecord(String rawPayload,
                               String errorMessage,
                               long failureTimestamp
) implements Serializable {}
