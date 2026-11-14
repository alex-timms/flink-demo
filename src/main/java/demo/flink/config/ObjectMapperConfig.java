package demo.flink.config;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ObjectMapperConfig {

    public static ObjectMapper objectMapper() {
       return new ObjectMapper();
    }
}
