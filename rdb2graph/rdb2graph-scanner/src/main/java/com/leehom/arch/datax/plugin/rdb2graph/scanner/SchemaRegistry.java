/**
 * %datax-graph%
 * %v1.0%
 */
package com.leehom.arch.datax.plugin.rdb2graph.scanner;

import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.DbSchema;

/**
 * @类名: SchemaRegistry
 * @说明: schema注册, 支持文件(序列化)，数据库，zk，nacos, redis等等
 *
 * @author   leehom
 * @Date	 2022年4月27日 下午3:57:27
 * 修改记录：
 *
 * @see 	 
 */
public interface SchemaRegistry {

	public DbSchema registry(DbSchema schema);
	public DbSchema load(String uri);
}
