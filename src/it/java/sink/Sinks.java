package sink;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;


public class Sinks {


    /**
     * Stores everything it receives in a static map, so the test can assert on it.
     * Works because Flink's local mini cluster runs in the same JVM as the test.
     * With parallelism above 1, arrival order is not guaranteed.
     */
    public static class CollectingSink<T> implements Sink<T> {
        private static final Map<String, List<Object>> STORE = new ConcurrentHashMap<>();
        private final String name;

        public CollectingSink(String name) {
            this.name = name;
        }

        public static List<Object> results(String name) {
            return STORE.getOrDefault(name, List.of());
        }

        public static void clear() {
            STORE.clear();
        }

        @Override
        public SinkWriter<T> createWriter(WriterInitContext context) {
            return new SinkWriter<T>() {
                @Override
                public void write(T element, Context ctx) {
                    STORE.computeIfAbsent(name, k -> new CopyOnWriteArrayList<>()).add(element);
                }

                @Override
                public void flush(boolean endOfInput) {
                }

                @Override
                public void close() {
                }
            };
        }
    }
}
