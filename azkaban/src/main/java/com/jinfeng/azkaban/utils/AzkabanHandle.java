package com.jinfeng.azkaban.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.client.RestOperations;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

/**
 * @package: com.mobvista.dmp.server.azkaban
 * @author: wangjf
 * @date: 2019/4/19
 * @time: 下午4:27
 * @email: jinfeng.wang@mobvista.com
 * @phone: 152-1062-7698
 */
@Component
public class AzkabanHandle {
    private static Logger log = LoggerFactory.getLogger(AzkabanHandle.class);
    private String URI;

    private String userName;

    private String password;

    private String SESSION_ID = "6102b053-8720-4940-8baf-0bed38748821";

    private static RestTemplate restTemplate;

    static {
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(2000);
        requestFactory.setReadTimeout(2000);
        restTemplate = new RestTemplate(requestFactory);
    }

    //  默认azkaban信息
    //  private static String url = "https://dataplatform.mobvista.com:8443";

    private static String url = "https://52.3.17.31:8443";

    public AzkabanHandle() {
        this(url, "jinfeng.wang@mobvista.com", "Wangjf&19920627");
    }

    // 构造函数传参azkaban信息
    public AzkabanHandle(String URI, String userName, String password) {
        this.URI = URI;
        this.userName = userName;
        this.password = password;
    }

    /**
     * 登录
     */
    public boolean login() throws InterruptedException {
        try {
            SSLUtil.turnOffSsl();
        } catch (KeyManagementException | NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        HttpHeaders hs = new HttpHeaders();
        hs.add("Content-Type", "application/x-www-form-urlencoded; charset=utf-8");
        hs.add("X-Requested-With", "XMLHttpRequest");
        LinkedMultiValueMap<String, String> linkedMultiValueMap = new LinkedMultiValueMap<>();
        linkedMultiValueMap.add("action", "login");
        linkedMultiValueMap.add("username", userName);
        linkedMultiValueMap.add("password", password);

        HttpEntity<LinkedMultiValueMap<String, String>> httpEntity = new HttpEntity<>(linkedMultiValueMap, hs);
        String responsResultString = null;
        try {
            responsResultString = restTemplate.postForObject(URI, httpEntity, String.class);
        } catch (Exception e) {
            int tryNum = 3;
            for (int i = 0; i < tryNum; i++) {
                Thread.sleep(1000);
                try {
                    responsResultString = restTemplate.postForObject(URI, httpEntity, String.class);
                } catch (Exception ee) {
                }
            }
        }
        JSONObject resJson;
        if (responsResultString != null) {
            responsResultString = responsResultString.replace(".", "_");
            resJson = JSON.parseObject(responsResultString);
        } else {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("status", "failure");
            resJson = jsonObject;
        }
        if ("success".equals(resJson.getString("status"))) {
            SESSION_ID = resJson.getString("session_id");
            log.info("azkaban login success:{}", resJson);
            return true;
        } else {
            log.warn("azkabna login failure:{}", resJson);
            return false;
        }
    }

    /**
     * 执行Flow
     *
     * @param projectName project 名称
     * @param flowName    flow 名称
     * @return str
     */
    public String startFlow(String projectName, String flowName, String jobId, String json) throws Exception {
        SSLUtil.turnOffSsl();
        HttpHeaders hs = new HttpHeaders();
        hs.add("Content-Type", "application/x-www-form-urlencoded; charset=utf-8");
        hs.add("X-Requested-With", "XMLHttpRequest");
        LinkedMultiValueMap<String, Object> linkedMultiValueMap = new LinkedMultiValueMap<>();
        linkedMultiValueMap.add("session.id", SESSION_ID);
        linkedMultiValueMap.add("ajax", "executeFlow");
        linkedMultiValueMap.add("project", projectName);
        linkedMultiValueMap.add("flow", flowName);
        linkedMultiValueMap.add("flowOverride[jobId]", jobId);
        linkedMultiValueMap.add("flowOverride[jobJson]", json);
        String res = restTemplate.postForObject(URI + "/executor", linkedMultiValueMap, String.class);
        log.info("azkaban start flow:{}", res);
        return JSON.parseObject(res).getString("execid");
    }

    /**
     * resume Flow
     *
     * @param execId
     * @return str
     */
    public String resumeFlow(String execId) throws NoSuchAlgorithmException, KeyManagementException {
        SSLUtil.turnOffSsl();
        HttpHeaders hs = new HttpHeaders();
        hs.add("Content-Type", "application/x-www-form-urlencoded; charset=utf-8");
        hs.add("X-Requested-With", "XMLHttpRequest");
        LinkedMultiValueMap<String, Object> linkedMultiValueMap = new LinkedMultiValueMap<>();
        log.info("SESSION_ID -->> " + SESSION_ID);
        linkedMultiValueMap.add("session.id", SESSION_ID);
        linkedMultiValueMap.add("ajax", "resumeFlow");
        linkedMultiValueMap.add("execid", execId);
        String res = restTemplate.getForObject(URI + "/executor", String.class, linkedMultiValueMap);
        log.info("azkaban start flow:{}", res);
        return JSON.parseObject(res).toJSONString();
    }

    /**
     * fetch FlowInfo
     *
     * @param execId
     * @return str
     */
    public String flowInfo(String execId) throws NoSuchAlgorithmException, KeyManagementException {
        SSLUtil.turnOffSsl();
        //  LinkedMultiValueMap<String, Object> linkedMultiValueMap = new LinkedMultiValueMap<>();
        //  linkedMultiValueMap.add("session.id", SESSION_ID);
        //  linkedMultiValueMap.add("ajax", "flowInfo");
        //  linkedMultiValueMap.add("execid", execId);

        Map<String, Object> uriVariables = new HashMap<>();
        uriVariables.put("session.id", SESSION_ID);
        uriVariables.put("ajax", "flowInfo");
        uriVariables.put("execid", execId);

        log.info("SESSION_ID -->> " + SESSION_ID);

        String url = URI + "/executor?ajax={ajax}&session.id={session.id}&execid={execid}";
        String res = restTemplate.getForObject(url, String.class, uriVariables);
        log.info("azkaban flowInfo:{}", res);
        return JSON.parseObject(res).toJSONString();
    }

    public void uploadZip(String projectName, String filePath) throws Exception {
        SSLUtil.turnOffSsl();
        FileSystemResource resource = new FileSystemResource(new File(filePath));
        LinkedMultiValueMap<String, Object> linkedMultiValueMap = new LinkedMultiValueMap<>();
        linkedMultiValueMap.add("session.id", SESSION_ID);
        linkedMultiValueMap.add("ajax", "upload");
        linkedMultiValueMap.add("project", "demo_wangjf");
        linkedMultiValueMap.add("file", resource);
        String postForObject = restTemplate.postForObject(URI + "/manager", linkedMultiValueMap, String.class);
        log.info("azkaban upload file:{}", postForObject);
    }
}