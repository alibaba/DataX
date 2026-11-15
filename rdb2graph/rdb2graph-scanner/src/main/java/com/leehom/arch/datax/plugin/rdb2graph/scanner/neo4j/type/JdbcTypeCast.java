/**
 * %%
 * %%
 */
package com.leehom.arch.datax.plugin.rdb2graph.scanner.neo4j.type;

import java.sql.JDBCType;

/**
 * @类名: JdbcTypeCast
 * @说明: jdbc类型转换为neo4j类型
 *
 * @author   leehom
 * @Date	 2020年9月3日 下午3:42:26
 * 修改记录：
 *
 * @see 	 
 */
@FunctionalInterface
public interface JdbcTypeCast {
	
	public Neo4jType cast(JDBCType jdbcType);
	
}
