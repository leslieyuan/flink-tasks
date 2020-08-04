package com.cestc.flink.udf;

import org.apache.flink.table.functions.ScalarFunction;

public class AddTwoUdf extends ScalarFunction {
    public int eval(Integer i ) {
        return (i == null) ? 0 : i + 2;
    }
}
