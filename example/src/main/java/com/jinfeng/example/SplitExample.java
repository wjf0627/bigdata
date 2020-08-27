package com.jinfeng.example;

/**
 * @package: com.jinfeng.example
 * @author: wangjf
 * @date: 2019-07-31
 * @time: 14:36
 * @email: jinfeng.wang@mobvista.com
 * @phone: 152-1062-7698
 */
public class SplitExample {
    public static void main(String[] args) {
        String url = "https://play.google.com/store/apps/category/BUSINESS";

        String[] strs = url.split("\\/");
        System.out.println(strs[strs.length - 1]);
    }
}
