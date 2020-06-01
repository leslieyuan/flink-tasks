package com.ly.log4j;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * @author yuanlong
 * @version 1.0
 * @description
 * @date 2020/6/1 11:52
 */

public class Logs {
    private static final Logger log = LoggerFactory.getLogger(Logs.class);

    public static void init(String topic) throws IOException {
        Properties pro = new Properties();
        pro.load(Logs.class.getResourceAsStream("/log4j.properties"));
        pro.setProperty("log4j.appender.kafka.topic", topic);
        PropertyConfigurator.configure(pro);

        log.info("initial log success");
    }

}