/**
 * %datax-graph%
 * %v1.0%
 */
package com.leehom.arch.datax.plugin.rdb2graph.scanner;

import org.junit.Before;
import org.junit.Test;

import com.leehom.arch.datax.plugin.rdb2graph.common.BeanUtils;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.graph.Graph;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.DbSchema;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.ds.BaseDataSourceBean;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.ds.DruidConnectionProperties;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.ds.DruidDataSourceBean;


/**
 * @类名: MysqlScannerTest
 * @说明: 数据库扫描引测试
 * 
 * @author leehom
 * @Date 2012-8-29 下午5:55:46 
 * 
 * 
 * @see
 */
public class MysqlScannerTest {

	private AbstractDbScanner mysqlScanner;
	private DruidDataSourceBean dsBean;
	
//    db: sakila
//    url: jdbc:mysql://localhost:3306/?remarks=true&useInformationSchema=false&serverTimezone=Asia/Shanghai
//    tbs: 
//    username: root
//    password: root
//    driverClass: com.mysql.cj.jdbc.Driver
//    filters: stat
//    maxActive: 20
//    initialSize: 1
//    maxWait: 60000
//    minIdle: 1
//    timeBetweenEvictionRunsMillis: 60000
//    minEvictableIdleTimeMillis: 300000
//    validationQuery: select 'x'
//    testWhileIdle: true
//    testOnBorrow: false
//    testOnReturn: false
//    poolPreparedStatements: true
//    maxOpenPreparedStatements: 20
    
	@Before
	public void init() {
		dsBean = new DruidDataSourceBean();
	    DruidConnectionProperties connProps = new DruidConnectionProperties();
	    connProps.setDb("sakila");
	    connProps.setUrl("jdbc:mysql://localhost:3306/?remarks=true&useInformationSchema=false&serverTimezone=Asia/Shanghai");
	    connProps.setUserName("root");
	    connProps.setPassword("123456");
	    connProps.setDriverClass("com.mysql.jdbc.Driver");
	    //
	    dsBean.setConnProps(connProps);
	    //
	    mysqlScanner = new MysqlScanner();
	    mysqlScanner.setConnProps(connProps);
	}
	
	@Test
	public void testDbScan() throws Exception {
		DbSchema dbmds = mysqlScanner.scan(dsBean);
		BeanUtils.printBean(dbmds);
	}
	
	@Test
	public void testSetLinkTable() throws Exception {
		DbSchema dbmds = mysqlScanner.scan(dsBean);
		BeanUtils.printBean(dbmds);
	}	

	// 构建表连接图测试
	@Test
	public void testBuildGraph() throws Exception {
		DbSchema dbSch = mysqlScanner.scan(dsBean);
		Graph g = dbSch.buildTableGraph();
		g.print();
	}
	
}
