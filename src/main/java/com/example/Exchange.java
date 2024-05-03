package com.example;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Exchange {
    public static final String EXCHANGE_NAME = "my_exchange";
    private static final Map<String, String> routingKeyToTopicMap = new HashMap<>();
    private static final Map<String, String> topicToRoutingKeyMap = new HashMap<>();

    static {
        // Load routing keys and topics from the configuration file
        loadRoutingKeysAndTopics();
    }

    private static void loadRoutingKeysAndTopics() {
        try {
            Properties config = new Properties();
            try (FileInputStream fis = new FileInputStream("config.properties")) {
                config.load(fis);
                boolean topicsSection = false;
                boolean routingKeysSection = false;
                for (String key : config.stringPropertyNames()) {
                    String value = config.getProperty(key);
                    if (value.startsWith("#")) {
                        // Skip comment lines
                        if (value.startsWith("#Topics")) {
                            topicsSection = true;
                            routingKeysSection = false;
                        } else if (value.startsWith("#RoutingKeys")) {
                            topicsSection = false;
                            routingKeysSection = true;
                        } else {
                            topicsSection = false;
                            routingKeysSection = false;
                        }
                    } else {
                        if (topicsSection) {
                            String topicKey = key;
                            String topic = value;
                            String routingKey = config.getProperty(topicKey.replace("Topic", "RoutingKey"));
                            if (routingKey != null) {
                                routingKeyToTopicMap.put(routingKey, topic);
                                topicToRoutingKeyMap.put(topic, routingKey);
                            }
                        } else if (routingKeysSection) {
                            // Skip routing keys for now, as we only need them to be associated with topics
                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void publishToExchange(String routingKey, String data) {
        // Publish data to the exchange using the provided routing key
        String topic = routingKeyToTopicMap.get(routingKey);
        if (topic != null) {
            System.out.println("Published to exchange '" + EXCHANGE_NAME + "' with routing key '" + routingKey + "' and topic '" + topic + "': " + data);
        } else {
            System.out.println("Invalid routing key: " + routingKey);
        }
    }

    public static String generateRoutingKey(String topic) {
        // Generate routing key based on the provided topic
        return topicToRoutingKeyMap.get(topic);
    }
}
