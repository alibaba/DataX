/**
 * %datax-graph%
 * %v1.0%
 */
package com.leehom.arch.datax.plugin.rdb2graph.scanner;

import java.io.File;

import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Files;
import com.leehom.arch.datax.plugin.rdb2graph.common.serializer.Serializer;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.config.ScannerSerializerConfig;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.DbSchema;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.TableMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.constraint.fk.FKConstraintMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.ds.BaseDataSourceBean;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.ds.DruidConnectionProperties;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.ds.DruidDataSourceBean;


/**
 * @类名: SchemaLoaderTest
 * @说明: 数据库模式载入测试
 * 
 * @author leehom
 * @Date 2012-8-29 下午5:55:46 
 * 
 * 
 * @see
 */
public class SchemaSerTest {

	private Serializer ser;
	private AbstractDbScanner dbScanner;
	private DruidDataSourceBean dsBean;

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
	    //
	    dsBean.setConnProps(connProps);
	    //
	    dbScanner = new MysqlScanner();
	    dbScanner.setConnProps(connProps);
	}
	
	@Test
	public void testRdbSchemaSer() throws Exception {
		//
		DbSchema schema = dbScanner.scan(dsBean);
		byte[] bytes = ser.Marshal(schema);
		File f = new File("sakila.xml");
		Files.write(bytes, f);
		
	}
	
	// 设置连接表
	// mysql
	@Test
	public void testMysqlRdbSchemaLinkTable() throws Exception {
		//
		DbSchema schema = dbScanner.scan(dsBean);
		// 连接表
		String filmCategory = "film_category";
		String filmCategoryFk = "fk_film_category_film";
		TableMetadata tFilmCategory = schema.findTable(filmCategory);
		FKConstraintMetadata fkFC = tFilmCategory.findFk(filmCategoryFk);
		schema.setLinkTable(tFilmCategory, fkFC);
		//
		String filmActor = "film_actor";
		String filmActorFk = "fk_film_actor_film";
		TableMetadata tFilmActor = schema.findTable(filmActor);
		FKConstraintMetadata fkFA = tFilmActor.findFk(filmActorFk);
		schema.setLinkTable(tFilmActor, fkFA);
		//
		byte[] bytes = ser.Marshal(schema);
		File f = new File("sakila.xml");
		Files.write(bytes, f);
		
	}
	
}
