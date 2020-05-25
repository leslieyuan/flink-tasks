package com.ly.sql;

import java.io.Serializable;

/**
 * @author yuanlong
 * @version 1.0
 * @description
 * @date 2020/5/25 17:10
 */

public class UserTime implements Serializable {

    private String user;
    private Long time;

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return "UserTime{" +
                "user='" + user + '\'' +
                ", time=" + time +
                '}';
    }
}