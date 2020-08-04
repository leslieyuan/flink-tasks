package com.cestc.flink.udf;

import org.apache.flink.table.functions.ScalarFunction;

public class HashCodeUdf extends ScalarFunction {

    private int factor;

    public int eval(String s) {
        return s == null ? 0 : s.hashCode() * factor;
    }

    public HashCodeUdf(int factor) {
        this.factor = factor;
    }
}
