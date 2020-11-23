package com.jinfeng.azkaban.example;

import com.jinfeng.azkaban.utils.AzkabanHandle;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

/**
 * @package: com.jinfeng.azkaban.example
 * @author: wangjf
 * @date: 2020/11/17
 * @time: 1:46 下午
 * @email: jinfeng.wang@mobvista.com
 * @phone: 152-1062-7698
 */
public class AzkabanDemo {

    public static void main(String[] args) throws KeyManagementException, NoSuchAlgorithmException, InterruptedException {
        AzkabanHandle azkabanHandle = new AzkabanHandle();
        //  azkabanHandle.resumeFlow("3969195");
        if (!azkabanHandle.login()) {
            System.out.println("False");
        }
        azkabanHandle.flowInfo("3969195");
        //  azkabanHandle.login();
    }
}
