package com.jinfeng.example;

/**
 * @package: mobvista.dmp.datasource.bundle.service
 * @author: wangjf
 * @date: 2020/11/3
 * @time: 2:38 下午
 * @email: jinfeng.wang@mobvista.com
 * @phone: 152-1062-7698
 */
public class BundleClass {
    private String bundleId;

    private String packageName;

    public BundleClass() {
    }

    public BundleClass(String bundleId, String packageName) {
        this.bundleId = bundleId;
        this.packageName = packageName;
    }

    public String getBundleId() {
        return bundleId;
    }

    public void setBundleId(String bundleId) {
        this.bundleId = bundleId;
    }

    public String getPackageName() {
        return packageName;
    }

    public void setPackageName(String packageName) {
        this.packageName = packageName;
    }
}
