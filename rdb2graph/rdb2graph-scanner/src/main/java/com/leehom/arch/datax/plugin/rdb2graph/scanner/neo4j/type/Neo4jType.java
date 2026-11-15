/**
 * %%
 * %%
 */
package com.leehom.arch.datax.plugin.rdb2graph.scanner.neo4j.type;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @类名: TypeEnum
 * @说明: 参数类型枚举
 *
 * @author   leehom
 * @Date	 2020年9月3日 下午6:01:12s
 * 修改记录：
 *
 * @see 	 
 */
/**
 * neo4j类型 v4.0
 * https://neo4j.com/docs/cypher-manual/4.0/syntax/values/
 */
public enum Neo4jType {

	BOOLEAN("boolean", "布尔型", Boolean.class),
	SHORT("short", "短整型", Short.class),
	INT("int", "整型", Integer.class),
	LONG("long", "长整型", Long.class),
	FLOAT("float", "浮点型", Float.class),
	DOUBLE("double", "双精度浮点型", Double.class),
	STRING("string", "字符串型", String.class),
	DATE("date", "日期型", Date.class),	
	ENUMCONST("enumconst", "枚举项", Enum.class),
	ENUM("enum", "枚举", Enum.class),
	// composite type
	STRINGS("strings", "字符串型数组", String[].class);
	
	private static Map<String, Class<?>> nameClazzMap; 
	private static Map<String, Neo4jType> typeMap; 
	
	static {
		nameClazzMap = new HashMap<>();
		nameClazzMap.put("BOOLEAN", Boolean.class);
		nameClazzMap.put("SHORT", Short.class);
		nameClazzMap.put("INT", Integer.class);
		nameClazzMap.put("LONG", Long.class);
		nameClazzMap.put("FLOAT", Float.class);
		nameClazzMap.put("DOUBLE", Double.class);
		nameClazzMap.put("STRING", String.class);
		nameClazzMap.put("DATE", Date.class);
		//
		typeMap = new HashMap<>(); 
		typeMap.put("BOOLEAN", BOOLEAN);
		typeMap.put("SHORT", SHORT);
		typeMap.put("INT", INT);
		typeMap.put("LONG", LONG);
		typeMap.put("FLOAT", FLOAT);
		typeMap.put("DOUBLE", DOUBLE);

	}
	
	// 获取类型
	public static Class<?> nameToClazz(String name) {
		return nameClazzMap.get(name);
	}
	
	//
	public static Neo4jType nameToType(String name) {
		return typeMap.get(name);
	}

	Neo4jType(String name, String alias, Class<?> clazz) {
		
	}

}
