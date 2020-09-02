package com.cestc.sqlsubmit.log4j;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author yuanlong
 * @version 1.0
 * @description
 * @date 2020/6/1 11:52
 */

public class Logs {
    private static final Logger LOG = LoggerFactory.getLogger(Logs.class);

    public static void init(String jobName) {
        CestcJsonLayout.setRwid(jobName);
        CestcJsonLayout.setRwLxDm("1");
        PropertyConfigurator.configure(Logs.class.getResourceAsStream("/log4j.properties"));
        LOG.info("log initial finished");
    }
}