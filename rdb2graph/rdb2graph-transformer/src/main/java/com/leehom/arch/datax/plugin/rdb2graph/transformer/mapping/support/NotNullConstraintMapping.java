/**
 * %datax-graph%
 * %v1.0%
 */
package com.leehom.arch.datax.plugin.rdb2graph.transformer.mapping.support;

import java.text.MessageFormat;

import org.neo4j.driver.Query;

import com.leehom.arch.datax.plugin.rdb2graph.scanner.neo4j.ds.Neo4jQueryPattern;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.neo4j.ds.QueryType;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.neo4j.ds.QueryWrapper;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.DbSchema;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.constraint.NotNullConstraintMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.transformer.mapping.SchemaMapping;

/**
 * @类名: UniqueConstraintMapping
 * @说明: 唯一约束映射
 *
 * @author   leehom
 * @Date	 2022年4月22日 下午4:43:58
 * 修改记录：
 *
 * @see 	 
 */
public class NotNullConstraintMapping implements SchemaMapping<NotNullConstraintMetadata> {

	@Override
	public QueryWrapper mapTo(DbSchema dbSchema, NotNullConstraintMetadata nnc) {
		//
		String cql = MessageFormat.format(Neo4jQueryPattern.CREATE_NN_CONSTRAINT, 
				nnc.getNnName(), 
				nnc.getTbmd().getName(), // label
				Neo4jQueryPattern.PROPERTTY_ALIAS+nnc.getField().getName()
				);
		Query query = new Query(cql);
		return QueryWrapper.wrap(query, QueryType.INSERT);
	}

}
