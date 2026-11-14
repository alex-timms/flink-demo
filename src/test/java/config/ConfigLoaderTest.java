package config;

import duplicateDetection.config.ConfigLoader;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class ConfigLoaderTest {

    @Test
    public void testConfigLoader() {
        assertThat(ConfigLoader.get("app.name")).isEqualTo("TEST");
        assertThat(ConfigLoader.getInt("app.version", 0)).isEqualTo(1);
    }
}
