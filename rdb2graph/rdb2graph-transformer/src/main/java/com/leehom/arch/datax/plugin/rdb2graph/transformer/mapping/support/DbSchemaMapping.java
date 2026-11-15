/**
 * %datax-graph%
 * %v1.0%
 */
package com.leehom.arch.datax.plugin.rdb2graph.transformer.mapping.support;

import java.util.Map;

import org.neo4j.driver.Query;

import com.google.common.collect.Maps;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.neo4j.ds.Neo4jQueryPattern;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.neo4j.ds.QueryType;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.neo4j.ds.QueryWrapper;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.DbSchema;
import com.leehom.arch.datax.plugin.rdb2graph.transformer.mapping.SchemaMapping;

/**
 * @类名: DbSchemaMapping
 * @说明: 数据库映射
 *
 * @author   leehom
 * @Date	 2022年4月22日 下午4:43:58
 * 修改记录：
 *
 * @see 	 
 */
public class DbSchemaMapping implements SchemaMapping<DbSchema> {

	@Override
	public QueryWrapper mapTo(DbSchema dbSchema, DbSchema schemaItem) {
		// 
		String dbName = dbSchema.getName();
		Map<String, Object> params = Maps.newHashMap();
		params.put("dbname", dbName);
		Query query = new Query(Neo4jQueryPattern.CREATE_DB, params);
		return QueryWrapper.wrap(query, QueryType.INSERT);
	}

}
