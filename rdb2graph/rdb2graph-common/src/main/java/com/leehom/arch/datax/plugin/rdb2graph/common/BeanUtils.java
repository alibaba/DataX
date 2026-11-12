/**
 * 
 */
package com.leehom.arch.datax.plugin.rdb2graph.common;

import java.io.PrintStream;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.Map;

/**
 * 类名: BeanUtils
 * 说明: Bean工具类
 *
 * @author   leehom 
 * @Date	 Nov 30, 2009 4:09:36 PM
 * 修改记录：
 *
 * @see 	 
 */
public class BeanUtils {
	
	/**
	 * @说明：输出Bean的属性/深输出Bean的属性
	 * 
	 * @param   '输出流'
	 * @author hjli
	 * 
	 * @异常： 
	 */
	public static void printBean(Object bean) {
		printBean(bean, System.out);
	}
	
	public static void printBean(Object bean, PrintStream out) {
		out.println(bean.toString()); 
	}
	
	public static void printBeanDeep(Object bean) {
		printBeanDeep(bean, System.out);
	}
	
	public static void printBeanDeep(Object bean, PrintStream out) {
		try {
			Map m = org.apache.commons.beanutils.BeanUtils.describe(bean);
			for (Object  o : m.keySet()) {  
				if(o==null){
					out.println("Null value field");
					continue;
				}
				out.println(o.toString()+":"+m.get(o));  
		  }
		} catch(Exception ex) {
			throw new RuntimeException(ex.getMessage());
		}
	}
	
	public static Long number2Long(Number num) {
		if(num!=null)
			return num.longValue();
		return null;

	}
	
	public static Integer number2Integer(Number num) {
		if(num!=null)
			return num.intValue();
		return null;

	}
	
	public static Double number2Double(Number num) {
		if(num!=null)
			return num.doubleValue();
		return null;

	}
	
	public static Short number2Short(Number num) {
		if(num!=null)
			return num.shortValue();
		return null;

	}
	
	public static Byte number2Byte(Number num) {
		if(num!=null)
			return num.byteValue();
		return null;

	}
	
	public static Double bigDecimal2Double(BigDecimal num) {
		if(num!=null)
			return num.doubleValue();
		return null;

	}
	
	public static Long bigDecimal2Long(BigDecimal num) {
		if(num!=null)
			return num.longValue();
		return null;

	}
	
	public static Integer bigDecimal2Integer(BigDecimal num) {
		if(num!=null)
			return num.intValue();
		return null;

	}
	
	public static Integer bigInteger2Integer(BigInteger num) {
		if(num!=null)
			return num.intValue();
		return null;

	}
	
	public static Long bigInteger2Long(BigInteger num) {
		if(num!=null)
			return num.longValue();
		return null;

	}
	
	public static Date stringToDate(String dateStr, Class<?> clazz) {
		if(dateStr==null)
			return null;
		return DateTimeUtils.StringToDate(dateStr);
		
	}
	
    public static double doublePrecision(double d, short precision) {
    	BigDecimal bg = new BigDecimal(d);
    	double d2 = bg.setScale(precision, BigDecimal.ROUND_HALF_DOWN).doubleValue();
    	return d2;
    }
    
	public static Class getTemplateType(Object obj) {
		Class clazz = null;
		Class<?> c = obj.getClass();
        Type t = c.getGenericSuperclass();
        if (t instanceof ParameterizedType) {
            Type[] p = ((ParameterizedType) t).getActualTypeArguments();
            if(p[0] instanceof ParameterizedType )
            	clazz = (Class) ((ParameterizedType) p[0]).getRawType();
            else
            	clazz = (Class) p[0];
        }
        return clazz;
		
	}
}
