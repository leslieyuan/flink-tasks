package com.ly.log4j;

/**
 * @author yuanlong
 * @version 1.0
 * @description
 * @date 2020/6/16 18:38
 */

public class LogItem {
    private final String rwid;
    private final String rwzt;
    private final String rwsj;
    private final String rwrz;
    private final String trance;

    public LogItem(String id, String zt, String sj, String rz, String trance) {
        this.rwid = id;
        this.rwzt = zt;
        this.rwsj = sj;
        this.rwrz = rz;
        this.trance = trance;
    }

    public String getRwid() {
        return rwid;
    }

    public String getRwzt() {
        return rwzt;
    }

    public String getRwsj() {
        return rwsj;
    }

    public String getRwrz() {
        return rwrz;
    }

    public String getTrance() {
        return trance;
    }
}