package com.cestc.flink.udf;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;

/**
 * UNIX_TIMETAMP function with millisecond
 * <p>
 * 例如输入为"2020-09-11 13:14:29.153", 输出为1599801269153
 * 错误输入返回0
 */
public class UnixtimestampMillisecond extends ScalarFunction {

    public long eval(String timeStr) {
        try {
            Timestamp timestamp = Timestamp.valueOf(timeStr);
            return timestamp.getTime();
        } catch (Exception e) {
            return 0;
        }
    }
}
