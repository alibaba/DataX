package com.leehom.arch.datax.plugin.rdb2graph.scanner.config;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;

import com.leehom.arch.datax.plugin.rdb2graph.scanner.neo4j.ds.Neo4jDao;

public class Neo4jDaoConfiguration {
	
	public static Driver neo4jDriver(final Neo4jDriverProperties driverProperties) {
		return GraphDatabase.driver(driverProperties.getUri(), 
				AuthTokens.basic(driverProperties.getUsername(),driverProperties.getPassword()));
	}
	
	public static Neo4jDao neo4jDao(final Neo4jDriverProperties driverProperties) {
		Driver driver = Neo4jDaoConfiguration.neo4jDriver(driverProperties);
		Neo4jDao neo4jDao =  new Neo4jDao();
		neo4jDao.setDatabase(driverProperties.getDatabase());
		neo4jDao.setDriver(driver);
		return neo4jDao;
	}
	
}
