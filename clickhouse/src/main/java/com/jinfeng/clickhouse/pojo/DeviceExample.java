package com.jinfeng.clickhouse.pojo;

/**
 * @package: com.jinfeng.clickhouse.pojo
 * @author: wangjf
 * @date: 2019-07-08
 * @time: 10:01
 * @emial: wjf20110627@163.com
 * @phone: 152-1062-7698
 */
public class DeviceExample {

    private String device_id;

    private String dt;

    private Integer flag;

    public DeviceExample() {
    }

    public DeviceExample(String device_id, String dt, Integer flag) {
        this.device_id = device_id;
        this.dt = dt;
        this.flag = flag;
    }

    public String getDevice_id() {
        return device_id;
    }

    public void setDevice_id(String device_id) {
        this.device_id = device_id;
    }

    public String getDt() {
        return dt;
    }

    public void setDt(String dt) {
        this.dt = dt;
    }

    public Integer getFlag() {
        return flag;
    }

    public void setFlag(Integer flag) {
        this.flag = flag;
    }
}
