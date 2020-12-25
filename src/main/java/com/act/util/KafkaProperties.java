package com.act.util;

import java.util.Properties;

public class KafkaProperties {
    public static Properties getKafkaProperties(String bootstrap, String groupId) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrap);
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "latest");
        properties.setProperty("enable.auto.commit", "false");

        return properties;
    }
}
