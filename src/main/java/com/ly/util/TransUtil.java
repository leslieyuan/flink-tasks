package com.ly.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class TransUtil {
    /**
     * Trans properties to map
     * @param properties need trans
     * @return map
     */
    public static Map<String, String> pro2Map(Properties properties) {
        Map<String, String> resultMap = new HashMap<>();
        properties.stringPropertyNames().forEach(
                (String k) -> resultMap.put(k, properties.getProperty(k)));
        return resultMap;
    }
}
