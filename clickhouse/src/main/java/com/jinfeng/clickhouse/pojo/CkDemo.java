package com.jinfeng.clickhouse.pojo;

/**
 * @package: com.jinfeng.kafka.utils.pojo
 * @author: wangjf
 * @date: 2019-07-10
 * @time: 16:16
 * @emial: wjf20110627@163.com
 * @phone: 152-1062-7698
 */
public class CkDemo {
    public long timestamp;
    public String level;
    public String message;

    public CkDemo() {
    }

    public CkDemo(long timestamp, String level, String message) {

        this.timestamp = timestamp;
        this.level = level;
        this.message = message;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
