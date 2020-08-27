package com.jinfeng.flink.entity;

import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Frozen;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Table(keyspace = "dmp_realtime_service", name = "dmp_user_features")
public class ReflectiveDeviceCase implements Serializable {

    @PartitionKey
    @Column(name = "device_id")
    private String device_id;

    @Column(name = "age")
    private String age;

    @Column(name = "gender")
    private String gender;

    @Column(name = "interest")
    private Set<String> interest;

    @Frozen
    @Column(name = "frequency")
    private Set<TupleValue> frequency;

    public ReflectiveDeviceCase(TupleType tupleType, Object field, List<?> field1) throws ClassCastException {
        this.device_id = (String) field;
        Set<String> set = new HashSet<>();

        for (int i = 0; i < field1.size(); i++) {
            set.add(field1.get(i).toString());
        }
        this.interest = set;
        /*
        this.age = (String) field1;
        this.gender = (String) field2;
         */
        /*
        Set<TupleValue> tset = new HashSet<>();
        if (Arrays.asList(field1).size() > 0) {
            String[] ts = Row.of(field1).toString().replace("[", "").replace("]", "").split(",");
            if (ts.length > 1) {
                for (int i = 0; i < ts.length; i += 2) {
                    String tag = ts[i];
                    String cnt = ts[i + 1];
                    TupleValue t = tupleType.newValue(tag, cnt);
                    //  Tuple2<String, Integer> t = new Tuple2<>(tag, cnt);
                    tset.add(t);
                }
                this.frequency = tset;
            }
        }

         */

        /*
        if (field1.size() > 0) {
            for (int i = 0; i < Arrays.asList(field1.get(0)).size(); i++) {
                //  Row t = (Row) Arrays.asList(field1.get(0)).get(i);
                //  Arrays.asList(frequency.get(0)).get(i)
                //  System.out.println(Arrays.asList(frequency.get(0)).get(i).getClass());
                //    String tag = ((Record) Arrays.asList(frequency.get(0)).get(i)).;
                //  RowTypeInfo schema = new RowTypeInfo(new TypeInformation[]{BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO}, new String[]{"tag","cnt"});
                //  TypeInformation<?>[] typeArray = new TypeInformation<?>[arity];
                //  typeArray = TypeExtractor.getForObject(Arrays.asList(field1.get(0)).get(i));
                String[] tcs = Row.of(Row.of(Arrays.asList(field1.get(0)).get(i))).toString().split(",");
                String tag = tcs[0];
                //  Arrays.asList(field1.get(0)).get(i)
                int cnt = Integer.parseInt(tcs[1]);
                TupleUtils t = new TupleUtils(tag, cnt);
                tset.add(t);
            }

            this.frequency = tset;
        }
         */
        /*
        Set<String> installSet = new HashSet<>();
        for (int i = 0; i < field1.size(); i++) {

            installSet.add(field1.get(i).toString());
        }
        this.install = installSet;
         */
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
