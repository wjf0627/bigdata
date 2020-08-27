package com.jinfeng.flink.entity;

import com.datastax.driver.core.*;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.FrozenValue;
import com.datastax.driver.mapping.annotations.Table;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @package: com.jinfeng.flink.entity
 * @author: wangjf
 * @date: 2019-06-25
 * @time: 17:32
 * @emial: wjf20110627@163.com
 * @phone: 152-1062-7698
 */
@Table(keyspace = "dmp_realtime_service", name = "dmp_user_features")
public class DevicePojo implements Serializable {
    @Column(name = "device_id")
    private String device_id;

    @Column(name = "age")
    private int age;

    @Column(name = "gender")
    private int gender;

    @Column(name = "install_apps")
    private Set<String> install;

    @Column(name = "interest")
    private Set<String> interest;

    @Column(name = "frequency")
    @FrozenValue
    private Set<TupleValue> frequency;
    //  private Set<Tuple2<String,String>> frequency;

    //  TupleType tupleType
    public DevicePojo(TupleType tupleType, Object device_id, Object age, Object gender, Object install, Object interest, List<?> frequency) {

        this.device_id = (String) device_id;
        this.age = Integer.parseInt((String) age);
        this.gender = Integer.parseInt((String) gender);
        Set<String> set = new HashSet<>();
        String[] install_row = Row.of(install).toString().replace("[", "").replace("]", "").split(",");
        for (int i = 0; i < install_row.length; i++) {
            set.add(install_row[i]);
        }
        this.install = set;
        Set<String> iset = new HashSet<>();
        String[] interest_row = Row.of(interest).toString().replace("[", "").replace("]", "").split(",");
        for (int i = 0; i < interest_row.length; i++) {
            iset.add(interest_row[i]);
        }
        this.interest = iset;

        //  UserType userType = keyspaceMetadata.getUserType("struct");
        //  Set<UDTValue> tset = new HashSet<>();
        //  Set<TupleUtils> tset = new HashSet<>();
        Set<TupleValue> tset = new HashSet<>();
        if (frequency.size() > 0) {
            String[] ts = Row.of(frequency.get(0)).toString().replace("[", "").replace("]", "").split(",");
            if (ts.length > 1) {
                for (int i = 0; i < ts.length; i += 2) {
                    //  UDTValue udtValue = userType.newValue();
                    String tag = ts[i].trim();
                    String cnt = ts[i + 1];
                    TupleValue t = tupleType.newValue(tag, cnt);
                    //  Tuple2<String, String> t = new Tuple2<>(tag, cnt);
                    //  TupleUtils t = new TupleUtils(tag, cnt);
                    //  udtValue.setString("tag", tag);
                    //  udtValue.setString("cnt", cnt);
                    //  TupleUtils t = new TupleUtils(tag, cnt);
                    tset.add(t);
                }
                this.frequency = tset;
            } else {
                this.frequency = new HashSet<>();
            }
        } else {
            this.frequency = new HashSet<>();
        }
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

    public Set<TupleValue> getFrequency() {
        return frequency;
    }

    public void setFrequency(Set<TupleValue> frequency) {
        this.frequency = frequency;
    }
}
