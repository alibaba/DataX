/**
 * %datax-graph%
 * %v1.0%
 */
package com.leehom.arch.datax.plugin.rdb2graph.transformer.mapping;

import java.util.Map;

/**
 * @类名: MappingRepository
 * @说明: 
 *
 * @author   leehom
 * @Date	 2022年4月21日 下午6:44:11
 * 修改记录：
 *
 * @see 	 
 */
public class MappingRepository {
	
	/** 映射子子映射实现*/
	private Map<Class, SchemaMapping> mappings;
	
	/**
	 * 说明：获取合适的映射实现
	 * @author leehom
	 * @param op
	 * @return
	 * 
	 */
	public SchemaMapping getMapping(Class clazz) {
		return mappings.get(clazz);
	}

	public void setMappings(Map<Class, SchemaMapping> mappings) {
		this.mappings = mappings;
	}
}
