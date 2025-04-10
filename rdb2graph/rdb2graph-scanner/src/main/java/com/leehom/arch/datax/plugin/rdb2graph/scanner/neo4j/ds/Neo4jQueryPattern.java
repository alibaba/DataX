package com.leehom.arch.datax.plugin.rdb2graph.scanner.neo4j.ds;


/**
 * @类名: Neo4jQueryPattern
 * @说明: neo4j查询
 *
 * @author   leehom
 * @Date	 2013-4-10 下午2:31:41
 * @修改记录：
 *
 * @see 	 
 */
public interface Neo4jQueryPattern {
	
	/** 数据库*/
	public static final String USE_DB = "USE $dbname";
	public static final String CREATE_DB = "CREATE DATABASE $dbname IF NOT EXISTS";
	

	/** node属性别名*/
	public static final String PROPERTTY_ALIAS = "n.";
	/** 关系属性别名*/
	public static final String REL_PROPERTTY_ALIAS = "r.";	
	
	/**
	 * 创建唯一约束 
	 * constraintname;labelname;properties
	 */
	public static final String CREATE_UNQ_CONSTRAINT = "CREATE CONSTRAINT {0} IF NOT EXISTS "
			+ "FOR (n:{1}) "
			+ "REQUIRE ({2}) IS UNIQUE";
	
	/** 创建非空约束，constraintname;labelname;properties*/
	public static final String CREATE_NN_CONSTRAINT = "CREATE CONSTRAINT {0} IF NOT EXISTS "
			+ "FOR (n:{1}) "
			+ "REQUIRE ({2}) IS NOT NULL";
	
	// node 索引，0, 索引名称；1, n:Lable; 2, n.x, 节点属性
	public static final String CREATE_NODE_INDEX = "CREATE INDEX {0} IF NOT EXISTS FOR (n:{1}) ON ({2})";
	// 关系 索引， 0, 索引名称；1, n:Lable; 2, n.x, 关系属性
	public static final String CREATE_REL_INDEX = "CREATE INDEX {0} IF NOT EXISTS  FOR ()-[n:{1}]-() ON ({2})";
	
	// node, 
	// 0, label; 1, 属性, {"key", "value"}
	public static final String CREATE_NODE = "CREATE (n:{0} {1})";
	public static final String UPDATE_NODE = "CREATE (n:{0} {1})";
	public static final String DELETE_NODE = "MATCH (n:{0} {1}) DELETE n";
	
	// rel，0，关系from node label；1，关系to node label；2， where， from/to；3, 关系type；4， 关系属性
	public static final String CREATE_REL = "MATCH (a:{0}), (b:{1}) WHERE {2} "
										  + "CREATE (a)-[r:{3} {4}]->(b) ";

	// query all
	public static final String MATCH_ALL = "MATCH (n) RETURN n";


}
