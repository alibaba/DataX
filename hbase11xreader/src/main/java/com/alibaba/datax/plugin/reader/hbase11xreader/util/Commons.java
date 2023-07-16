package com.alibaba.datax.plugin.reader.hbase11xreader.util;


import org.apache.commons.lang.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

//import java.util.Base64;

public class Commons {
	public static final String JSON_OBJECT = "JSONOBJECT";
	public static final String JSON_ARRAY = "JSONARRAY";
	public static final String STRING = "STRING";
	public static final String FAST_JSON_OBJECT = "FASTJSONOBJECT";
	public static final String FAST_JSON_ARRAY = "FASTJSONARRAY";


	/**
	 *
	 *功能：去除字符串中的特殊字符
	 *@return
	 */
	public static String parseLineStr(String str) {
		str = str.replaceAll("\n", "");
		str = str.replaceAll("\r", "");
		str = str.replaceAll("\n\r", "");
		str = str.replaceAll("\\\\n", "");
		str = str.replaceAll("\r\n", "");
		str = str.replaceAll("\t", "");
		str = str.replaceAll("\001", "");
		str = str.replaceAll("\r", "");
		return str;
	}

	public static String parseFieldStr(String valueStr) {
		if (StringUtils.isBlank(valueStr))
			return valueStr;
		valueStr = valueStr.replaceAll("\n", "");
		valueStr = valueStr.replaceAll("\r", "");
		valueStr = valueStr.replaceAll("\n\r", "");
		valueStr = valueStr.replaceAll("\r\n", "");
		valueStr = valueStr.replaceAll("\t", "");
		valueStr = valueStr.replaceAll("\001", "");
		valueStr = valueStr.replaceAll("\r", "");
		return valueStr;
	}








	public static String objectToString(Object object){
		if(object instanceof Integer){
			return ""+(Integer)object;
		}else if(object instanceof Long){
			return ""+(Long)object;
		}else if(object instanceof Double){
			return ""+(Double)object;
		}else if(object instanceof Float){
			return ""+(Double)object;
		}else if(object instanceof com.alibaba.fastjson.JSONObject){
			return ((com.alibaba.fastjson.JSONObject)object).toJSONString();
		}else if(object instanceof com.alibaba.fastjson.JSONArray){
			return ((com.alibaba.fastjson.JSONArray)object).toJSONString();
		}else{
			return (String)object;
		}

	}


	public static String getJSONType(String str) {



		try {
			com.alibaba.fastjson.JSONObject.parseObject(str);
			return JSON_OBJECT;
		} catch (Exception e) {
		}

		try {
			com.alibaba.fastjson.JSONObject.parseArray(str);
			return JSON_ARRAY;
		} catch (Exception e) {
		}
		return STRING;

	}
	/**
	 * check string type
	 * @param object
	 * @return
	 */
	public static String checkObjectType(Object object){
		try {
			JSONObject valueJsonObject = new JSONObject(object);
			if(valueJsonObject.has("bytes") && valueJsonObject.has("empty") && !valueJsonObject.getBoolean("empty")){
				throw new Exception("不是jsonObject");
			}
			return JSON_OBJECT;
		} catch (Exception e) {
		}

		try {
			JSONArray jsonArray = new JSONArray(object);
			return JSON_ARRAY;
		} catch (Exception e) {
		}
		if(object instanceof com.alibaba.fastjson.JSONObject){
			return FAST_JSON_OBJECT;
		}

		if(object instanceof com.alibaba.fastjson.JSONArray){
			return FAST_JSON_ARRAY;
		}

		return STRING;


	}

	public static String getStringByBytes(byte[] bytesValue){
		Object values =  Commons.deserialize(bytesValue);
		String valueStr = "";
		String type = Commons.checkObjectType(values);

		if(type.equals(Commons.FAST_JSON_OBJECT)){
			com.alibaba.fastjson.JSONObject object= (com.alibaba.fastjson.JSONObject)values;
			valueStr = object.toJSONString();
		}else if(type.equals(Commons.JSON_OBJECT)){
			JSONObject valueJsonObject = new JSONObject(values);
			valueStr = valueJsonObject.toString();
		}else if(type.equals(Commons.JSON_ARRAY)){
			JSONArray jsonArray = new JSONArray(values);
			valueStr = jsonArray.toString();
		}else if(type.equals(Commons.FAST_JSON_ARRAY)){
			com.alibaba.fastjson.JSONArray object= (com.alibaba.fastjson.JSONArray)values;
			valueStr = object.toJSONString();
		}else{

			valueStr=(String)values;
		}
		return valueStr;
	}

	public static String getMd5(String plainText) {
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(plainText.getBytes());
			byte b[] = md.digest();

			int i;

			StringBuffer buf = new StringBuffer("");
			for (int offset = 0; offset < b.length; offset++) {
				i = b[offset];
				if (i < 0)
					i += 256;
				if (i < 16)
					buf.append("0");
				buf.append(Integer.toHexString(i));
			}
			// 32位加密
			return buf.toString();
			// 16位的加密
			// return buf.toString().substring(8, 24);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			return null;
		}

	}

	// 时间戳到日期，日期到时间戳

	public static String stampToDate(long lt) {
		String res;
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = new Date(lt);
		res = simpleDateFormat.format(date);
		return res;
	}




	public static long dateToStamp(String s) throws ParseException {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = simpleDateFormat.parse(s);
		long ts = date.getTime();
		return ts;
	}




	public static byte[] objectToBytes(Object obj) {
		byte[] bytes = null;
		ByteArrayOutputStream bo = null;
		ObjectOutputStream oo = null;
		try {

			bo = new ByteArrayOutputStream();
			oo = new ObjectOutputStream(bo);
			oo.writeObject(obj);

			bytes = bo.toByteArray();

			bo.close();
			oo.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			closeStream(bo);
			closeStream(oo);
		}
		return bytes;
	}



	public static Object deserialize(byte[] bytes) {
		Object result = null;
		try {
			result = byteToObject(bytes);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}





//	public static Object byteToObject(byte[] bytes) {
//		Object obj = null;
//		ByteArrayInputStream bi = null;
//		ObjectInputStream oi = null;
//		try {
//
//			bi = new ByteArrayInputStream(bytes);
//			oi = new ObjectInputStream(bi);
//
//			obj = oi.readObject();
//			bi.close();
//			oi.close();
//		} catch (Exception e) {
//			obj = new String(bytes);
//			System.out.println("字节数组转对象报错,格式错误bytes:{}" + e);
//		} finally {
//			closeStream(bi);
//			closeStream(oi);
//		}
//		return obj;
//	}


	public static Object byteToObject(byte[] bytes) {
		Object obj = null;
		ByteArrayInputStream bi = null;
		ObjectInputStream oi = null;
		try {
			bi = new ByteArrayInputStream(bytes);
			oi = new ObjectInputStream(bi);
			obj = oi.readObject();
			bi.close();
			oi.close();
		} catch (Exception e) {
			obj = new String(bytes, Charset.forName("UTF-8"));
		} finally {
			closeStream(bi);
			closeStream(oi);
		}
		return obj;
	}

	private static void closeStream(Closeable closeable) {
		if (closeable != null) {
			try {
				closeable.close();
			} catch (Exception e) {
				closeable = null;
				System.out.println(e);
			} finally {
				if (closeable != null) {
					closeable = null;
				}
			}
		}
	}





}
