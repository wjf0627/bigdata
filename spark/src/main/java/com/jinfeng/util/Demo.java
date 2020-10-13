package com.jinfeng.util;

import java.util.Set;

/**
 * @package: com.jinfeng.util
 * @author: wangjf
 * @date: 2020/9/25
 * @time: 1:49 下午
 * @email: jinfeng.wang@mobvista.com
 * @phone: 152-1062-7698
 */

public class Demo {
    String devid;
    Set<String> region;

    Demo(String devid, Set<String> region) {
        this.devid = devid;
        this.region = region;
    }

    public String getDevid() {
        return devid;
    }

    public void setDevid(String devid) {
        this.devid = devid;
    }

    public Set<String> getRegion() {
        return region;
    }

    public void setRegion(Set<String> region) {
        this.region = region;
    }

    public Demo() {

    }
}
