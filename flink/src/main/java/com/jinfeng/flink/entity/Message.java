package com.jinfeng.flink.entity;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;

import java.io.Serializable;

/**
 * @package: com.jinfeng.model
 * @author: wangjf
 * @date: 2019/3/28
 * @time: 下午5:51
 * @email: wjf20110627@163.com
 * @phone: 152-1062-7698
 */
@Table(keyspace = "example", name = "message")
public class Message implements Serializable {

    private static final long serialVersionUID = 1123119384361005680L;

    @Column(name = "body")
    private String message;

    public Message() {
        this(null);
    }

    public Message(String word) {
        this.message = word;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String word) {
        this.message = word;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof Message) {
            Message that = (Message) other;
            return this.message.equals(that.message);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return message.hashCode();
    }
}