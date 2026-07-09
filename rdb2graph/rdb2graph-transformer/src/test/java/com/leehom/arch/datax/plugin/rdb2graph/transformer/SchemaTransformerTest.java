/**
 * %datax-graph%
 * %v1.0%
 */
package com.leehom.arch.datax.plugin.rdb2graph.transformer;

import java.io.InputStream;
import java.net.URI;

import org.junit.Before;
import org.junit.Test;

import com.leehom.arch.datax.plugin.rdb2graph.common.ByteAndStreamUtils;
import com.leehom.arch.datax.plugin.rdb2graph.common.ResourceLoaderUtil;
import com.leehom.arch.datax.plugin.rdb2graph.common.serializer.Serializer;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.AbstractDbScanner;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.MysqlScanner;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.config.Neo4jDaoConfiguration;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.config.Neo4jDriverProperties;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.config.ScannerSerializerConfig;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.neo4j.ds.Neo4jDao;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.DbSchema;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.ds.DruidConnectionProperties;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.ds.DruidDataSourceBean;
import com.leehom.arch.datax.plugin.rdb2graph.transformer.config.SchemaMappingBeanConfig;


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
public class SchemaTransformerTest {

	private SchemaTransformer transformer;
	private DruidDataSourceBean dsBean;
	private Serializer ser;
	private AbstractDbScanner dbScanner;
	private Neo4jDriverProperties props;
	private Neo4jDao neo4jDao;
	
	@Before
	public void init() {
		// 
		ser = ScannerSerializerConfig.rdbSchemaXmlSerializer();
		//
		dsBean = new DruidDataSourceBean();
	    DruidConnectionProperties connProps = new DruidConnectionProperties();
	    connProps.setDb("sakila");
	    connProps.setUrl("jdbc:mysql://localhost:3306/?remarks=true&useInformationSchema=false&serverTimezone=Asia/Shanghai");
	    connProps.setUserName("root");
	    connProps.setPassword("123456");
	    connProps.setDriverClass("com.mysql.jdbc.Driver");
	    dsBean.setConnProps(connProps);
	    //
	    dbScanner = new MysqlScanner();
	    dbScanner.setConnProps(connProps);
	    //
		props = new Neo4jDriverProperties();
		props.setDatabase("sakila");
		props.setUri(URI.create("bolt://192.168.1.20:7687"));
		props.setUsername("neo4j");
		props.setPassword("neo4j1234");
		neo4jDao = Neo4jDaoConfiguration.neo4jDao(props);
		//
		transformer = new SchemaTransformer();
		transformer.setDbScanner(dbScanner);
		transformer.setMappingRepository(SchemaMappingBeanConfig.mappingRepository());
		transformer.setNeo4jDao(neo4jDao);
	}
	
	@Test	
	public void testTransform() throws Exception {
		transformer.transform(dsBean);
	}

	
	@Test	
	public void testTransformScheam() throws Exception {
		InputStream is = ResourceLoaderUtil.getResourceStream("sakila.xml");
		byte[] bytes = ByteAndStreamUtils.StreamToBytes(is);
		DbSchema schema = (DbSchema)ser.Unmarshal(bytes);
		transformer.transform(schema);
	}
}
