/**
 * %datax-graph%
 * %v1.0%
 */
package com.leehom.arch.datax.plugin.rdb2graph.scanner;

import java.text.MessageFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.leehom.arch.datax.plugin.rdb2graph.common.BeanUtils;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.neo4j.ds.Neo4jDao;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.neo4j.ds.Neo4jQueryPattern;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.neo4j.ds.ParamsUtils;



/**
 * @类名: Neo4jDaoTest
 * @说明: 数据库扫描引测试
 * 
 * @author leehom
 * @Date 2012-8-29 下午5:55:46 
 * 
 * 
 * @see
 */
public class Neo4jDaoTest {

	private Neo4jDao neo4jDao;

	@Test
	public void testCreateGraph() throws Exception {
		//
		Map<String, Object> params = new HashMap<>();
		params.put("dbname", "WCXX");
		Query query = new Query(Neo4jQueryPattern.CREATE_DB, params);
		neo4jDao.executeQuery(query);
	}
	
	@Test
	public void testMatchALL() throws Exception {
		// 
		Query query = new Query(Neo4jQueryPattern.MATCH_ALL);
		List<Record> rs = neo4jDao.runQuery(query);
		BeanUtils.printBean(rs);
	}
	
	@Test
	public void testShowIndex() throws Exception {
		// 
		Query query = new Query("show indexes");
		List<Record> rs = neo4jDao.runQuery(query);
		BeanUtils.printBean(rs);
	}
	
	// 测试构建约束
	@Test
	public void testCreateCONSTRAINT() throws Exception {
		// 
		Map<String, Object> params = Maps.newHashMap();
		params.put("constraintname", "CONSTRAINT1");
		params.put("labelname", "yyy");
		// 索引字段
		params.put("properties", "n.x1, n.x2");
		Query query = new Query("CREATE CONSTRAINT constraint1 IF NOT EXISTS "
				+ "FOR (n:labelname) REQUIRE (n.x1, n.x2) IS UNIQUE", params);
		neo4jDao.executeQuery(query);

	}

	@Test
	public void testRunInTransaction() throws Exception {
		String cql1 = MessageFormat.format(Neo4jQueryPattern.CREATE_NODE, "test", "{name: 'Andy1', title: 'Developer1'}");
    	Query query1 = new Query(cql1);
    	
		String cql2 = MessageFormat.format(Neo4jQueryPattern.CREATE_NODE, "test", "{name: 'Andy2', title: 'Developer2'}");
    	Query query2 = new Query(cql2);
    	
    	neo4jDao.runInTransaction(Lists.newArrayList(query1, query2));
	}
	
	// 
	@Test
	public void testRunInTransaction2() throws Exception {
		// 
		Map<String, Object> params = Maps.newHashMap();
		params.put("key1", "xxxxx");
		params.put("key2", 123);
		Date date = new Date();
        Instant instant = date.toInstant();
        ZoneId zoneId = ZoneId.systemDefault();
        LocalDateTime ldt = instant.atZone(zoneId).toLocalDateTime();
		params.put("key3", date);
		//
		String jsonProps = ParamsUtils.params2String(params);
		
		String cql1 = MessageFormat.format(Neo4jQueryPattern.CREATE_NODE, "test", jsonProps);
    	Query query1 = new Query(cql1);;
    	
    	neo4jDao.runInTransaction(Lists.newArrayList(query1));
	}
	
}
