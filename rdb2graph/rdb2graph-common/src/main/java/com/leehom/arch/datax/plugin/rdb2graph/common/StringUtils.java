/**
 * %utils%
 * %1.0%
 */
package com.leehom.arch.datax.plugin.rdb2graph.common;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @类名: StringUtils
 * @说明: 
 *
 * @author   leehom
 * @Date	 2021年11月27日 下午4:47:54
 * 修改记录：
 *
 * @see 	 
 */
public class StringUtils {
	
	public static final String EMPTY = "";

	/**
	 * Pads a String <code>s</code> to take up <code>n</code> characters,
	 * padding with char <code>c</code> on the left (<code>true</code>) or on
	 * the right (<code>false</code>). Returns <code>null</code> if passed a
	 * <code>null</code> String.
	 **/
	public static String paddingString(String s, int n, char c, boolean paddingLeft) {
		if (s == null) {
			return s;
		}
		int add = n - s.length(); // may overflow int size... should not be a
									// problem in real life
		if (add <= 0) {
			return s;
		}
		StringBuffer str = new StringBuffer(s);
		char[] ch = new char[add];
		Arrays.fill(ch, c);
		if (paddingLeft) {
			str.insert(0, ch);
		} else {
			str.append(ch);
		}
		return str.toString();
	}

	public static String padLeft(String s, int n, char c) {
		return paddingString(s, n, c, true);
	}

	public static String padRight(String s, int n, char c) {
		return paddingString(s, n, c, false);
	}

	public static boolean isInt(String s) {
		try {
			Integer.parseInt(s);
			return true;
		} catch (Exception ex) {
			return false;
		}
	}

	/**
	 * @说明：字符串编码
	 *
	 * @author leehom
	 * @param src
	 * @return
	 * 
	 * 		异常：
	 */
	public static String strCoderGBk(String src) {
		try {
			if (src == null)
				return null;
			return new String(src.getBytes("ISO-8859-1"), "utf8");
		} catch (UnsupportedEncodingException e) {
			return null;
		}
	}

	public static String stringFromSet(Set<String> ss, String spliter) {
		if (ss == null || ss.size() == 0)
			return null;
		StringBuffer result = new StringBuffer();
		String[] a = ss.toArray(new String[0]);
		//
		result.append(a[0]);
		for (int i = 1; i < a.length; i++) {
			result.append(spliter);
			result.append(a[i]);
		}
		return result.toString();
	}

	// 字符串->字符数组
	public static Set<String> str2Set(String s, String spliter) {
		if (s == null || s.equals(""))
			return null;
		String[] sa = s.split(spliter);
		List<String> sl = Arrays.asList(sa);
		Set<String> set = new HashSet<String>(sl);
		return set;
	}
	
	// 字符串转换字符串数组
	public static String[] str2Strings(String s, String spliter) {
		if (s == null || s.equals(""))
			return new String[0];
		String[] sa = s.split(spliter);
		String[] ss = new String[sa.length];
		for(int i=0;i<sa.length;i++) {
			ss[i] = sa[i].trim();
	
		}
		return ss;
	}
	
	public static List<String> array2List(String[] strArry) {
		return Arrays.asList(strArry);
	}
	
	public static List<String> str2List(String s, String spliter) {
		String[] arrayStr = str2Strings(s, spliter);
		return Arrays.asList(arrayStr);
	}

	public static String array2Str(Object[] objs) {
		return array2Str(objs, "\"", ",");
	}

	public static String list2Str(List<Object> objs, String quotor, String seq) {
		if (objs == null || objs.size() == 0)
			return "";
		String s = "";
		for (int i = 0; i < objs.size(); i++) {
			Object obj = objs.get(i);
			if (obj instanceof String) 
				s = s + quotor + objs.get(i).toString() + quotor;
			else
				s = s + objs.get(i).toString();
			if (i != objs.size() - 1)
				s = s + seq;
		}
		return s;
	}

	public static String array2Str(Object[] objs, String quotor, String seq) {
		if (objs == null || objs.length == 0)
			return "";
		String s = "";
		for (int i = 0; i < objs.length; i++) {
			s = s + quotor + objs[i].toString() + quotor;
			if (i != objs.length - 1)
				s = s + seq;
		}
		return s;
	}

	// 去除文本中的的html标签
	public static String removeTagFromText(String content) {
		Pattern p = null;
		Matcher m = null;
		String value = null;
		// 去掉<>标签
		p = Pattern.compile("(<[^>]*>)");
		m = p.matcher(content);
		String temp = content;
		while (m.find()) {
			value = m.group(0);
			temp = temp.replace(value, "");
		}
		return temp;
	}

	public static String escape(String target, Map<String, String> escapes) {
		Set<String> key = escapes.keySet();
		for (Iterator<String> it = key.iterator(); it.hasNext();) {
			String s = (String) it.next();
			target = target.replaceAll(s, escapes.get(s));
		}
		return target;
	}
	
    public static boolean isBlank(String str) {
        int strLen;
        if (str == null || (strLen = str.length()) == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if ((!Character.isWhitespace(str.charAt(i)))) {
                return false;
            }
        }
        return true;
    }

    public static boolean isNotBlank(String str) {
        return !isBlank(str);
    }
	
	public static boolean isNotEmpty(String str) {
		return str!=null && !"".equals(str);
	}
	
	public static boolean isEmpty(String str) {
		return str==null || "".equals(str);
	}

}
