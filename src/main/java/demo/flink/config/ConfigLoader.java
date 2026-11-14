package demo.flink.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public final class ConfigLoader {

    private static final Properties PROPERTIES = new Properties();
    private static final String CONFIG_FILE = "config.properties";

    // Static block runs once when the class is first loaded into memory
    static {
        try (InputStream input = ConfigLoader.class.getClassLoader().getResourceAsStream(CONFIG_FILE)) {
            if (input == null) {
                throw new RuntimeException("Configuration file not found in resources: " + CONFIG_FILE);
            }
            PROPERTIES.load(input);
        } catch (IOException e) {
            throw new ExceptionInInitializerError("Failed to load configuration file: " + e.getMessage());
        }
    }

    private ConfigLoader() {
    }

    // Retrieve a String value
    public static String get(String key) {
        return PROPERTIES.getProperty(key);
    }

    // Helper for integer values
    public static int getInt(String key, int defaultValue) {
        String value = PROPERTIES.getProperty(key);
        return (value != null) ? Integer.parseInt(value.trim()) : defaultValue;
    }

    // Helper for boolean values
    public static boolean getBoolean(String key, boolean defaultValue) {
        String value = PROPERTIES.getProperty(key);
        return (value != null) ? Boolean.parseBoolean(value.trim()) : defaultValue;
    }
}
