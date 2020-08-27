package com.jinfeng.flink.entity;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

@Table(keyspace = "dmp_realtime_service", name = "dmp_user_features")
public class ReflectiveDevice {

    @Column(name = "device_id")
    private String device_id;

    @Column(name = "age")
    private int age;

    @Column(name = "gender")
    private int gender;

    @Column(name = "install")
    private String[] install;

    @Column(name = "interest")
    private String[] interest;

    @Column(name = "frequency")
    //  private List<TupleUtils> frenquency;
    private String[] frenquency;

    public ReflectiveDevice(Object device_id, Object age, Object gender, Object install, Object interest, Object frenquency) {
        this.device_id = (String) device_id;
        this.age = (int) age;
        this.gender = (int) gender;
        this.install = (String[]) install;
        this.interest = (String[]) interest;
        this.frenquency = (String[]) frenquency;
    }

    public String getDevice_id() {
        return device_id;
    }

    public void setDevice_id(String device_id) {
        this.device_id = device_id;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public int getGender() {
        return gender;
    }

    public void setGender(int gender) {
        this.gender = gender;
    }

    public String[] getInstall() {
        return install;
    }

    public void setInstall(String[] install) {
        this.install = install;
    }

    public String[] getInterest() {
        return interest;
    }

    public void setInterest(String[] interest) {
        this.interest = interest;
    }

    public String[] getFrenquency() {
        return frenquency;
    }

    public void setFrenquency(String[] frenquency) {
        this.frenquency = frenquency;
    }
}
