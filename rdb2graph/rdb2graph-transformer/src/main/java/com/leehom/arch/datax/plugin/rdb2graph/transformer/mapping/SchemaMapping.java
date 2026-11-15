/**
 * %datax-graph%
 * %v1.0%
 */
package com.leehom.arch.datax.plugin.rdb2graph.transformer.mapping;

import com.leehom.arch.datax.plugin.rdb2graph.scanner.neo4j.ds.QueryWrapper;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.DbSchema;

/**
 * @类名: SchemaMapping
 * @说明: schema构件映射
 *
 * @author   leehom
 * @Date	 2022年4月21日 下午6:52:43
 * 修改记录：
 *
 * @see 	 
 */
public interface SchemaMapping<T> {
	
	/**
	 * 说明：映射rdb模式构件，生成neo4j query
	 *
	 * @author leehom
	 * @param op
	 * @param sch  索引特性
	 * @return
	 * 
	 */
	public QueryWrapper mapTo(DbSchema dbSchema, T schemaItem);
	
}
