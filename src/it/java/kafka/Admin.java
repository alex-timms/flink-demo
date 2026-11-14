package kafka;

import org.apache.kafka.clients.admin.AdminClient;

import java.util.Map;

public class Admin {

    public static AdminClient createAdminClient(String bootstrap) {
        return AdminClient.create(
                Map.<String, Object>of("bootstrap.servers", bootstrap));
    }
}
