package com.cestc.sqlsubmit.log4j;

/**
 * @author yuanlong
 * @version 1.0
 * @description 自定义日志layout类
 * @date 2020/6/16 18:36
 */
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Layout;
import org.apache.log4j.spi.LoggingEvent;

import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;

public class CestcJsonLayout extends Layout {
    private static final DateTimeFormatter DEFAULT_DATE_FORMAT_ = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private static final String TWO_WHITE_SPACE = "  ";
    private static final String FILTERED_MESSAGE = "some-no-need-log";
    private static String rwid;
    private static String rwLxDm;
    private final ObjectMapper om;
    private String lastLog = "";

    public CestcJsonLayout() {
        this.om = new ObjectMapper();
        this.om.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    public void activateOptions() {
    }

    public boolean ignoresThrowable() {
        return false;
    }

    public static void setRwid(String rwid) {
        CestcJsonLayout.rwid = rwid;
    }
    public static void setRwLxDm(String rwLxDm) {
        CestcJsonLayout.rwLxDm = rwLxDm;
    }

    @Override
    public String format(LoggingEvent loggingEvent) {
        if (rwid == null) {
            return FILTERED_MESSAGE;
        }
        if (checkDumplicate(loggingEvent.getMessage().toString())) {
            return FILTERED_MESSAGE;
        }
        Timestamp timestamp = new Timestamp(loggingEvent.getTimeStamp());
        String stacktrace = "";
        if (loggingEvent.getThrowableStrRep() != null) {
            stacktrace = String.join("\n", loggingEvent.getThrowableStrRep());
        }
        StringBuilder rwrz = new StringBuilder(loggingEvent.getLevel().toString())
                .append(TWO_WHITE_SPACE)
                .append(loggingEvent.getFQNOfLoggerClass())
                .append(TWO_WHITE_SPACE)
                .append(loggingEvent.getMessage() + stacktrace);

        if (rwrz.toString().contains("Number of retries has been exhausted")) {
            return FILTERED_MESSAGE;
        }
        LogItem li = new LogItem(
                rwid,
                rwLxDm,
                timestamp.toLocalDateTime().format(DEFAULT_DATE_FORMAT_),
                rwrz.toString()
        );

        String out;
        try {
            out = om.writeValueAsString(li);
        } catch (JsonProcessingException e) {
            return "JsonLayout - ERROR formatting log message\n";
        }

        return out;
    }

    private boolean checkDumplicate(String log) {
        if (this.lastLog.equals(log)) {
            return true;
        } else {
            this.lastLog = log;
            return false;
        }
    }
}