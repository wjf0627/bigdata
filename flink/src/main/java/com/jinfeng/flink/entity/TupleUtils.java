package com.jinfeng.flink.entity;

public class TupleUtils {
    private String tag;
    private String cnt;

    public TupleUtils(String tag, String cnt) {
        this.tag = tag;
        this.cnt = cnt;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getCnt() {
        return cnt;
    }

    public void setCnt(String cnt) {
        this.cnt = cnt;
    }
}
