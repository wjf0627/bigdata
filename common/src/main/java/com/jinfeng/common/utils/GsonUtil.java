package com.jinfeng.common.utils;

import com.google.gson.*;

/**
 * @package: com.jinfeng.utils
 * @author: wangjf
 * @date: 2019/3/19
 * @time: 下午2:43
 * @email: wjf20110627@163.com
 * @phone: 152-1062-7698
 */
public class GsonUtil {
    public static JsonElement toJsonTree(Object obj) {
        if (obj != null) {
            Gson gson = new Gson();
            return gson.toJsonTree(obj);
        }
        return null;
    }

    public static String toJson(Object obj) {
        if (null == obj) {
            return null;
        }
        Gson gson = new Gson();
        return gson.toJson(obj);
    }

    public static <T> T fromJson(JsonElement json, Class<T> clazz) {
        if (json == null || clazz == null) {
            return null;
        }
        Gson gson = new Gson();
        return gson.fromJson(json, clazz);
    }

    /**
     * 将字符串转换为JsonArray
     *
     * @param str
     * @return
     */
    public static JsonArray String2JsonArray(String str) {
        if (null != str && !"".equals(str)) {
            JsonParser parser = new JsonParser();
            JsonElement element = parser.parse(str);
            return element.getAsJsonArray();
        }
        return new JsonArray();
    }

    /**
     * 将字符串转换为JsonObject
     *
     * @param str
     * @return
     */
    public static JsonObject String2JsonObject(String str) {
        if (null != str && !"".equals(str)) {
            JsonParser parser = new JsonParser();
            JsonElement element = parser.parse(str);
            return element.getAsJsonObject();
        }
        return null;
    }
}
