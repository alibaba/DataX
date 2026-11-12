package com.leehom.arch.datax.plugin.rdb2graph.scanner.neo4j.ds;

import java.util.List;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.SessionConfig.Builder;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.exceptions.Neo4jException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.leehom.arch.datax.plugin.rdb2graph.common.StringUtils;

import lombok.Getter;
import lombok.Setter;

/**
 * @类名: Neo4jDao
 * @说明: neo4j dao
 *
 * @author   leehom
 * @Date	 2022年4月22日 上午11:13:36
 * 修改记录：
 *
 * @see 	 
 * 
 * TODO
 *   1. 返回支持反射转换为bean
 */
public class Neo4jDao {

	private final static Logger log = LoggerFactory.getLogger(Neo4jDao.class);
	
	@Setter
	@Getter
	private String database;

	@Setter
	private Driver driver;

	// 查询，返回结果集
	public List<Record> runQuery(Query query) {
		
		Builder builder = SessionConfig.builder().withDefaultAccessMode(AccessMode.READ);
		SessionConfig sessionConfig;
		if(StringUtils.isEmpty(database))
			sessionConfig = builder.build();
		else
			sessionConfig = builder.withDatabase(database).build();
		try (Session session = driver.session(sessionConfig)) {
			return session.run(query).list();
		}

	}
	
	// 查询，返回一个结果
	public Record runQuerySingle(Query query) {

		SessionConfig sessionConfig = SessionConfig.builder().withDefaultAccessMode(AccessMode.READ).withDatabase(database).build();
		try (Session session = driver.session(sessionConfig)) {
			return session.run(query).single();
		}

	}
	
	// 执行
	public void executeQuery(Query query) {

		SessionConfig sessionConfig = sessionConfig(AccessMode.WRITE);
		try (Session session = driver.session(sessionConfig)) {
			session.run(query);
		}

	}
	
	// 事务
	public void runInTransaction(List<Query> queries) {

		SessionConfig sessionConfig = SessionConfig.builder().withDefaultAccessMode(AccessMode.WRITE).withDatabase(database).build();
		try (Session session = driver.session(sessionConfig)) {
			try(Transaction t = session.beginTransaction()) {
				for(Query q : queries) {
					t.run(q);
				}
				t.commit();
			}
		}

	}
	
	public void reTryRunInTransaction(List<Query> queries, int retries) {
		int r = 0;
		while(true) {
			try {
				SessionConfig sessionConfig = SessionConfig.builder().withDefaultAccessMode(AccessMode.WRITE).withDatabase(database).build();
				try (Session session = driver.session(sessionConfig)) {
					try(Transaction t = session.beginTransaction()) {
						for(Query q : queries) {
							t.run(q);
						}
						t.commit();
					}
				}
				break;
			} catch (Neo4jException e) {
				r++;
				if(r>retries) {
					log.error("neo4j批量query异常，重试次数超限[{}]，{}", r, e.getMessage());
					break;
				}
				log.error("neo4j批量query异常[{}]，{}", r, e.getMessage());
				try {
					Thread.sleep(300);
				} catch (InterruptedException e1) {
	
				}
			} 
		} // end while

	}
	
	private SessionConfig sessionConfig(AccessMode mode) {
		Builder builder = SessionConfig.builder().withDefaultAccessMode(mode);
		SessionConfig sessionConfig;
		if(StringUtils.isEmpty(database))
			sessionConfig = builder.build();
		else
			sessionConfig = builder.withDatabase(database).build();
		return sessionConfig;
	}
}

