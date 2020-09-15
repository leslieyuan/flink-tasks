//package com.ly.log;
//
//import org.apache.log4j.PropertyConfigurator;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//public class Logs {
//    private static final Logger LOG = LoggerFactory.getLogger(Logs.class);
//
//    public static void init(String jobName) {
//        PropertyConfigurator.configure(Logs.class.getResourceAsStream("/log4j.properties"));
//        LOG.info("log initial finished");
//    }
//}
