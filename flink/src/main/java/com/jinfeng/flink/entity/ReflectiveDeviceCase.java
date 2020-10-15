package com.jinfeng.flink.entity;

import com.datastax.driver.core.TupleType;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.*;

@Table(keyspace = "dmp_realtime_service", name = "dmp_user_features")
public class ReflectiveDeviceCase implements Serializable {

    @PartitionKey
    @Column(name = "device_id")
    private String device_id;

    @Column(name = "age")
    private String age;

    @Column(name = "gender")
    private String gender;

    @Column(name = "install_apps")
    private Set<String> install;

    @Column(name = "interest")
    private Set<String> interest;

    @Column(name = "frequency")
    private Map<String, Integer> frequency;

    public ReflectiveDeviceCase(TupleType tupleType, Object deviceId, Object age, Object gender, String[] install_list, String[] interest_tag, Row[] frequency_cnt) throws ClassCastException {
        this.device_id = (String) deviceId;
        this.age = (String) age;
        this.gender = (String) gender;

        Set<String> installSet = new HashSet<>(Arrays.asList(install_list));
        this.install = installSet;

        Set<String> interestSet = new HashSet<>(Arrays.asList(interest_tag));
        this.interest = interestSet;

        Map<String, Integer> map = new HashMap<>();
        if (frequency_cnt != null && frequency_cnt.length > 0) {
            for (Row row : frequency_cnt) {
                map.put(row.getField(0).toString(), Integer.parseInt(row.getField(1).toString()));
            }
            this.frequency = map;
        }
    }

    public String getDevice_id() {
        return device_id;
    }

    public void setDevice_id(String device_id) {
        this.device_id = device_id;
    }

    public String getAge() {
        return age;
    }

    public void setAge(String age) {
        this.age = age;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public Set<String> getInstall() {
        return install;
    }

    public void setInstall(Set<String> install) {
        this.install = install;
    }

    public Set<String> getInterest() {
        return interest;
    }

    public void setInterest(Set<String> interest) {
        this.interest = interest;
    }

    public Map<String, Integer> getFrequency() {
        return frequency;
    }

    public void setFrequency(Map<String, Integer> frequency) {
        this.frequency = frequency;
    }
}
