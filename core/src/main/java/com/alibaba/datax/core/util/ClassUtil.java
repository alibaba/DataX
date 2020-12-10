package com.alibaba.datax.core.util;

import java.lang.reflect.Constructor;

public final class ClassUtil {

	/**
	 * 通过反射构造类对象
	 * 
	 * @param className
	 *            反射的类名称
	 * @param t
	 *            反射类的类型Class对象
	 * @param args
	 *            构造参数
	 * 
	 * */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static <T> T instantiate(String className, Class<T> t,
			Object... args) {
		try {
			Constructor constructor = (Constructor) Class.forName(className)
					.getConstructor(ClassUtil.toClassType(args));
			return (T) constructor.newInstance(args);
		} catch (Exception e) {
			throw new IllegalArgumentException(e);
		}
	}

	private static Class<?>[] toClassType(Object[] args) {
		Class<?>[] clazzs = new Class<?>[args.length];

		for (int i = 0, length = args.length; i < length; i++) {
			clazzs[i] = args[i].getClass();
		}

		return clazzs;
	}

}
