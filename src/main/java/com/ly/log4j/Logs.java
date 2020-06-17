package com.ly.log4j;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author yuanlong
 * @version 1.0
 * @description
 * @date 2020/6/1 11:52
 */

public class Logs {
    private static final Logger log = LoggerFactory.getLogger(Logs.class);

    public static void init() {
        PropertyConfigurator.configure(Logs.class.getResourceAsStream("/log4j.properties"));
        log.info("initial log success");
    }

}