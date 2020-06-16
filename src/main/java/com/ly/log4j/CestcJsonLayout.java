package com.ly.log4j;

/**
 * @author yuanlong
 * @version 1.0
 * @description
 * @date 2020/6/16 18:36
 */
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Layout;
import org.apache.log4j.spi.LoggingEvent;

import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;


/**
 * yuanlong
 */
public class CestcJsonLayout extends Layout {
    private static final DateTimeFormatter DEFAULT_DATE_FORMAT_ = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS");
    private static String rwid;
    private static String rwzt;
    private final ObjectMapper om;

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

    public static void setRwzt(String rwzt) {
        CestcJsonLayout.rwzt = rwzt;
    }


    @Override
    public String format(LoggingEvent loggingEvent) {
        Timestamp timestamp = new Timestamp(loggingEvent.getTimeStamp());
        String stacktrace = null;
        if (loggingEvent.getThrowableStrRep() != null) {
            stacktrace = String.join("\n", loggingEvent.getThrowableStrRep());
        }

        LogItem li = new LogItem(
                rwid,
                rwzt,
                timestamp.toLocalDateTime().format(DEFAULT_DATE_FORMAT_),
                loggingEvent.getMessage().toString(),
                stacktrace
        );

        String out;

        try {
            out = om.writeValueAsString(li);
        } catch (JsonProcessingException e) {
            return "JsonLayout - ERROR formatting log message\n";
        }

        return out + "\n";
    }
}