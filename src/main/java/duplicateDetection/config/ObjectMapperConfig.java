package duplicateDetection.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.connector.kafka.source.KafkaSource;

public class ObjectMapperConfig {

    public static ObjectMapper objectMapper() {
       return new ObjectMapper();
    }
}
