/**
 * 
 */
package com.leehom.arch.datax.plugin.rdb2graph.scanner.config;

import java.util.HashMap;
import java.util.Map;

import com.leehom.arch.datax.plugin.rdb2graph.common.serializer.Serializer;
import com.leehom.arch.datax.plugin.rdb2graph.common.serializer.xstream.XmlSerializerXStreamImpl;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.DbSchema;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.FieldMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.TableMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.constraint.NotNullConstraintMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.constraint.UniqueConstraintMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.constraint.fk.FKConstraintMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.index.IndexMetadata;

/**
 * @类名: ScannerSerializerConfig
 * @说明: 序列化器配置
 *
 * @author   leehom
 * @Date	 2019年10月8日 下午4:21:34
 * 修改记录：
 *
 * @see 	 
 */
public class ScannerSerializerConfig {

	public static Serializer rdbSchemaXmlSerializer() {
		XmlSerializerXStreamImpl ser = new XmlSerializerXStreamImpl();
		// typeAlias
		Map<String, Class> typeAlias = new HashMap<String, Class>();
		// schema
		typeAlias.put("schema", DbSchema.class);
		// TableMetadata
		typeAlias.put("table", TableMetadata.class);
		// FieldMetadata
		typeAlias.put("field", FieldMetadata.class);
		// IndexMetadata
		typeAlias.put("index", IndexMetadata.class);
		// UniqueConstraintMetadata
		typeAlias.put("uq", UniqueConstraintMetadata.class);
		// FKConstraintMetadata
		typeAlias.put("fk", FKConstraintMetadata.class);
		// NotNullConstraintMetadata
		typeAlias.put("nn", NotNullConstraintMetadata.class);
		ser.setTypeAlias(typeAlias);
		return ser;
	}

}
