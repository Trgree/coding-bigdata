package org.ace.spark.task.utils;

import java.io.UnsupportedEncodingException;

/**
 * Created by Liangsj on 2018/4/3.
 */
public class StringUtils {
    public static String escapeExprSpecialWord(String str) {
        if (str !=null && str.trim().length() != 0) {
            String[] fbsArr = { "\\", "$", "(", ")", "*", "+", ".", "[", "]", "?", "^", "{", "}", "|" };
            for (String key : fbsArr) {
                if (str.contains(key)) {
                    str = str.replace(key, "\\" + key);
                }
            }
        }
        return str;
    }

    public static boolean isBlank(String str){
        return str == null || str.trim().length() == 0;
    }

    public static String decode(String str) throws UnsupportedEncodingException {
        if(str == null) {
            return null;
        }
        return new String(Base64.decode(str),"utf-8");
    }

    public static String encode(String str) throws UnsupportedEncodingException {
        if(str == null) {
            return null;
        }
        return  new String(Base64.encode(str.getBytes("utf-8")));
    }
}
