package org.ace.utils;

import java.util.regex.Pattern;

public class StringUtil {

	public static boolean isNull(String str){
		return str == null || str.trim().equals("");
	}
	
	public static int str2int(String str, int defVal){
		int result = defVal;
		try {
			result = Integer.parseInt(str);
		} catch (Exception e) {
		}
		return result;
	}
	

	public static float str2float(String str, float defVal){
		float result = defVal;
		try {
			result = Float.parseFloat(str);
		} catch (Exception e) {
		}
		return result;
	}
	

	public static double str2Double(String str, double defVal){
		double result = defVal;
		try {
			result = Double.parseDouble(str);
		} catch (Exception e) {
		}
		return result;
	}
	

	public static Long str2Long(String str, long defVal){
		long result = defVal;
		try {
			result = Long.parseLong(str);
		} catch (Exception e) {
		}
		return result;
	}
	
	/**
	 * 是否包含中文
	 * @param str
	 * @return
	 */
	public static boolean isContainChinese(String str) {
		return Pattern.compile("[\u4e00-\u9fa5]").matcher(str).find();
	}
}
