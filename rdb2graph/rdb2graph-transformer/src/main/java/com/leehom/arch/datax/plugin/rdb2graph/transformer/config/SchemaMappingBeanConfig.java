/**
 * 
 */
package com.leehom.arch.datax.plugin.rdb2graph.transformer.config;

import java.util.HashMap;
import java.util.Map;

import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.constraint.NotNullConstraintMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.constraint.UniqueConstraintMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.index.NodeIndexMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.index.RelIndexMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.transformer.mapping.MappingRepository;
import com.leehom.arch.datax.plugin.rdb2graph.transformer.mapping.SchemaMapping;
import com.leehom.arch.datax.plugin.rdb2graph.transformer.mapping.support.NodeIndexMapping;
import com.leehom.arch.datax.plugin.rdb2graph.transformer.mapping.support.NotNullConstraintMapping;
import com.leehom.arch.datax.plugin.rdb2graph.transformer.mapping.support.RelIndexMapping;
import com.leehom.arch.datax.plugin.rdb2graph.transformer.mapping.support.UniqueConstraintMapping;

/**
 * @类名: SchemaMappingBeanConfig
 * @说明: 搜索映射bean配置
 *
 * @author   leehom
 * @Date	 2019年10月8日 下午4:21:34
 * 修改记录：
 *
 * @see 	 
 */
public class SchemaMappingBeanConfig {

	public static MappingRepository mappingRepository() {
		MappingRepository mappingRepo = new MappingRepository();
		//
		Map<Class, SchemaMapping> mappings = new HashMap<>();
		// 创建数据库屏蔽
		// mappings.put(DbSchema.class, new DbSchemaMapping());
		// 创建唯一约束
		mappings.put(UniqueConstraintMetadata.class, new UniqueConstraintMapping());
		// 创建非空约束
		mappings.put(NotNullConstraintMetadata.class, new NotNullConstraintMapping());
		// 索引
		mappings.put(NodeIndexMetadata.class, new NodeIndexMapping());
		mappings.put(RelIndexMetadata.class, new RelIndexMapping());
		//
		mappingRepo.setMappings(mappings);
		//
		return mappingRepo;
	}
	
}
