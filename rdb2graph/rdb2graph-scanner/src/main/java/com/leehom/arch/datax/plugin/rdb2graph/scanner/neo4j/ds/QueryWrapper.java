/**
 * %datax-graph%
 * %v1.0%
 */
package com.leehom.arch.datax.plugin.rdb2graph.scanner.neo4j.ds;

import org.neo4j.driver.Query;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @类名: QueryWrapper
 * @说明: neo4j query包装
 *
 * @author   leehom
 * @Date	 2022年4月22日 下午6:07:23
 * 修改记录：
 *
 * @see 	 
 */
@Data
@AllArgsConstructor
public class QueryWrapper {

	private Query query;
	private QueryType queryType;

	public static QueryWrapper wrap(Query query, QueryType queryType) {
		return new QueryWrapper(query, queryType);
	}
}
