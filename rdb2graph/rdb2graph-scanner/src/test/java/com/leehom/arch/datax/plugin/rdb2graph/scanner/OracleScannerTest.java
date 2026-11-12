/**
 * %datax-graph%
 * %v1.0%
 */
package com.leehom.arch.datax.plugin.rdb2graph.scanner;

import java.io.File;
import java.io.InputStream;

import org.junit.Test;

import com.google.common.io.Files;
import com.leehom.arch.datax.plugin.rdb2graph.common.BeanUtils;
import com.leehom.arch.datax.plugin.rdb2graph.common.ByteAndStreamUtils;
import com.leehom.arch.datax.plugin.rdb2graph.common.ResourceLoaderUtil;
import com.leehom.arch.datax.plugin.rdb2graph.common.serializer.Serializer;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.DbSchema;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.TableMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.constraint.fk.FKConstraintMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.ds.BaseDataSourceBean;


/**
 * @类名: OracleScannerTest
 * @说明: oracle数据库扫描引测试
 * 
 * @author leehom
 * @Date 2012-8-29 下午5:55:46 
 * 
 * 
 * @see
 */
public class OracleScannerTest {

	private OracleScanner rdbScanner;
	private BaseDataSourceBean dsBean;
	private Serializer ser;
	
	@Test
	public void testDbScan() throws Exception {
		DbSchema dbmds = rdbScanner.scan(dsBean);
		dbmds.buildTableGraph().print();
	}

	// 设置连接表
	@Test
	public void testOracleRdbSchemaLinkTable() throws Exception {
		// 载入schema
		InputStream is = ResourceLoaderUtil.getResourceStream("wcc.xml");
		byte[] bytes = ByteAndStreamUtils.StreamToBytes(is);
		DbSchema schema = (DbSchema)ser.Unmarshal(bytes);
		// 连接表
		String partDocMaster = "WTPARTREFERENCELINK";
		String partDocMasterFk = "FK_IDA3A5_ID";
		TableMetadata table = schema.findTable(partDocMaster);
		FKConstraintMetadata fkFC = table.findFk(partDocMasterFk);
		schema.setLinkTable(table, fkFC);
		// 序列化输出
		byte[] bytesLinkTable = ser.Marshal(schema);
		File f = new File("wccx.xml");
		Files.write(bytesLinkTable, f);
		
	}
	
	@Test
	public void testRdbSchemaDeSer() throws Exception {
		//
		InputStream is = ResourceLoaderUtil.getResourceStream("wccx.xml");
		byte[] bytes = ByteAndStreamUtils.StreamToBytes(is);
		DbSchema schema = (DbSchema)ser.Unmarshal(bytes);
		BeanUtils.printBeanDeep(schema);
	}
	
	
}
