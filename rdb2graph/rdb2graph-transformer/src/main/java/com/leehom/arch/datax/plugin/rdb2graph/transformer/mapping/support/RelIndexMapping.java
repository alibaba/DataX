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
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.index.IndexMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.index.RelIndexMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.transformer.StringUtils;
import com.leehom.arch.datax.plugin.rdb2graph.transformer.mapping.SchemaMapping;

/**
 * @类名: BTreeIndexMapping
 * @说明: 唯一约束映射
 *
 * @author   leehom
 * @Date	 2022年4月22日 下午4:43:58
 * 修改记录：
 *
 * @see 	 
 */
public class RelIndexMapping implements SchemaMapping<RelIndexMetadata> {

	@Override
	public QueryWrapper mapTo(DbSchema dbSchema, RelIndexMetadata nindex) {
		IndexMetadata index = nindex.getIndexmd();
		//
		String cql = MessageFormat.format(Neo4jQueryPattern.CREATE_REL_INDEX, 
				index.getName(), 
				index.getTbmd().getName(), // label
				StringUtils.fieldNames2String(Neo4jQueryPattern.PROPERTTY_ALIAS, index.getFields()));
		Query query = new Query(cql);
		return QueryWrapper.wrap(query, QueryType.INSERT);
	}

}
