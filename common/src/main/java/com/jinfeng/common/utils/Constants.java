package com.jinfeng.common.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;

import java.util.regex.Pattern;

/**
 * @package: com.jinfeng.common.utils
 * @author: wangjf
 * @date: 2020/11/3
 * @time: 3:36 下午
 * @email: jinfeng.wang@mobvista.com
 * @phone: 152-1062-7698
 */
public class Constants {
    /**
     * bundle 和 packageName 对应关系文件
     */
    public static final String BUNDLE_PACKAGE_MAPPING = "bundle.package.mapping";

    public static final Pattern iosPkgPtn = Pattern.compile("^\\d+$");
    public static final Pattern adrPkgPtn = Pattern.compile("^(?=^.{3,255}$)[a-zA-Z0-9_][-a-zA-Z0-9_]{0,62}(\\.[a-zA-Z0-9_][-a-zA-Z0-9_]{0,62})+$");
    /**
     * mr广播字符串
     */
    public static final String MR_BROADCAST_STR = "mr.broadcast.str";

    public static JSONObject String2JSONObject(String str) {
        JSONObject jsonObject;
        if (StringUtils.isNotBlank(str)) {
            try {
                jsonObject = JSON.parseObject(str);
                if (jsonObject.isEmpty()) {
                    jsonObject = new JSONObject();
                }
            } catch (JSONException e) {
                jsonObject = new JSONObject();
            }
        } else {
            jsonObject = new JSONObject();
        }
        return jsonObject;
    }

    public static JSONArray String2JSONArray(String str) {
        JSONArray jsonArray;

        if (StringUtils.isNotBlank(str)) {
            try {
                jsonArray = JSON.parseArray(str);
                if (jsonArray.isEmpty()) {
                    new JSONArray();
                }
            } catch (JSONException e) {
                jsonArray = new JSONArray();
            }
        } else {
            jsonArray = new JSONArray();
        }
        return jsonArray;
    }
}
