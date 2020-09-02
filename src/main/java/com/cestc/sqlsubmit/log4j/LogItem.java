package com.cestc.sqlsubmit.log4j;

/**
 * @author yuanlong
 * @version 1.0
 * @description
 * @date 2020/6/16 18:38
 */


public class LogItem {
    private final String rwId;      // 任务ID
    private final String rwLxDm;    // 1表示实时计，2代表实时采集
    private final String sj;        // 任务时间
    private final String nr;        // 任务日志

    public LogItem(String id, String lxdm, String sj, String nr) {
        this.rwId = id;
        this.rwLxDm = lxdm;
        this.sj = sj;
        this.nr = nr;
    }

    public String getRwId() {
        return rwId;
    }

    public String getRwLxDm() {
        return rwLxDm;
    }

    public String getSj() {
        return sj;
    }

    public String getNr() {
        return nr;
    }
}