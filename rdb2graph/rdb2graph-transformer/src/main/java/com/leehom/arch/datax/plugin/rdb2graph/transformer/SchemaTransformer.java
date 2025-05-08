/**
 * %datax-graph%
 * %v1.0%
 */
package com.leehom.arch.datax.plugin.rdb2graph.transformer;

import java.util.List;

import com.leehom.arch.datax.plugin.rdb2graph.scanner.AbstractDbScanner;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.graph.Graph.Vertex;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.graph.VertexRelationship;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.neo4j.ds.Neo4jDao;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.neo4j.ds.QueryWrapper;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.DbSchema;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.TableMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.constraint.NotNullConstraintMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.constraint.UniqueConstraintMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.constraint.fk.FKConstraintMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.constraint.fk.FKField;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.ds.BaseDataSourceBean;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.index.IndexMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.index.NodeIndexMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.index.RelIndexMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.transformer.mapping.MappingRepository;
import com.leehom.arch.datax.plugin.rdb2graph.transformer.mapping.SchemaMapping;

import lombok.Data;

/**
 * @类名: SchemaTransformer
 * @说明: 模式转换器
 *
 * @author   leehom
 * @Date	 2022年4月21日 下午6:40:58
 * 修改记录：
 *
 * @see 	 
 * 
 */
@Data
public class SchemaTransformer {

	private AbstractDbScanner dbScanner;
	private MappingRepository mappingRepository;
	private Neo4jDao neo4jDao;
	
	/**
	 * @说明：转换模式(schema)
	 *        1. 数据库
	 *        2. 表构件，按表连接图
	 *        2.1 约束 唯一/非空
	 *        2.2 索引 btree
	 *
	 * @author leehom
	 * @param dbSchema
	 * 
	 */
	public void transform(BaseDataSourceBean dsBean) throws Exception {
		DbSchema schema = dbScanner.scan(dsBean);
		this.transform(schema);
	}
	public void transform(DbSchema dbSchem) {
		Vertex<TableMetadata>[] vxs = new Vertex[dbSchem.getTables().size()];
		// 顶点，邻接点
		for(int i=0;i<vxs.length;i++) {
			vxs[i] = new Vertex<>(dbSchem.getTables().get(i));
		}
		// 构建连接表
		for(int i=0;i<vxs.length;i++) {
			TableMetadata table= dbSchem.getTables().get(i);
			// 外键
			List<FKConstraintMetadata> fks = table.getFks();
			// 顶点点
			if(fks.size()==0) {
				this.transformVertex(dbSchem, vxs[i]);
				continue;
			}
			// 连接表
			if(table.isLinkTable()) {
				transformLinkRelationship(dbSchem, vxs[i]);
				continue;
			}
			// 顶点/关系
			int from = i;
			for(FKConstraintMetadata fk : fks) {
				// 
				int to = locate(fk.getRefTable().getName(), vxs);
				// 转换关系
				VertexRelationship<List<FKField>> vr = new VertexRelationship<List<FKField>>(from, to, fk.getFkFields());
				transformVertexRelationship(dbSchem, vxs, vr);
			}
		}

	}
	
	// 转换顶点
	public void transformVertex(DbSchema dbSchema, Vertex<TableMetadata> v) {
		// 唯一约束
		TableMetadata tbmd = v.getData();
		List<UniqueConstraintMetadata> uqcs = tbmd.getUnique();
		for(UniqueConstraintMetadata uqc : uqcs) {
			SchemaMapping<UniqueConstraintMetadata> mapping =  mappingRepository.getMapping(UniqueConstraintMetadata.class);
			QueryWrapper query = mapping.mapTo(dbSchema, uqc);
			neo4jDao.executeQuery(query.getQuery());
		}
		// 非空约束
		List<NotNullConstraintMetadata> nncs = tbmd.getNotNull();
		for(NotNullConstraintMetadata nnc : nncs) {
			SchemaMapping<NotNullConstraintMetadata> mapping =  mappingRepository.getMapping(NotNullConstraintMetadata.class);
			QueryWrapper query = mapping.mapTo(dbSchema, nnc);
			neo4jDao.executeQuery(query.getQuery());
		}
		// 索引
		List<IndexMetadata> indexmds = tbmd.getIndexes();
		for(IndexMetadata index : indexmds) {
			SchemaMapping<NodeIndexMetadata> mapping =  mappingRepository.getMapping(NodeIndexMetadata.class);
			QueryWrapper query = mapping.mapTo(dbSchema, new NodeIndexMetadata(index));
			neo4jDao.executeQuery(query.getQuery());
		}
	}
	// 转换连接表关系，node->edge->edge
	public void transformLinkRelationship(DbSchema dbSchema, Vertex<TableMetadata> v) {
		// 唯一约束
		TableMetadata tbmd = v.getData();
		List<UniqueConstraintMetadata> uqcs = tbmd.getUnique();
		for (UniqueConstraintMetadata uqc : uqcs) {
			SchemaMapping<UniqueConstraintMetadata> mapping = mappingRepository.getMapping(UniqueConstraintMetadata.class);
			QueryWrapper query = mapping.mapTo(dbSchema, uqc);
			neo4jDao.executeQuery(query.getQuery());
		}
		// 非空约束
		List<NotNullConstraintMetadata> nncs = tbmd.getNotNull();
		for (NotNullConstraintMetadata nnc : nncs) {
			SchemaMapping<NotNullConstraintMetadata> mapping = mappingRepository.getMapping(NotNullConstraintMetadata.class);
			QueryWrapper query = mapping.mapTo(dbSchema, nnc);
			neo4jDao.executeQuery(query.getQuery());
		}
		// 索引，按关系索引
		List<IndexMetadata> indexmds = tbmd.getIndexes();
		for (IndexMetadata index : indexmds) {
			SchemaMapping<RelIndexMetadata> mapping = mappingRepository.getMapping(RelIndexMetadata.class);
			QueryWrapper query = mapping.mapTo(dbSchema, new RelIndexMetadata(index));
			neo4jDao.executeQuery(query.getQuery());
		}

	}
	// 转换顶点/关系，node->edge->node
	public void transformVertexRelationship(DbSchema dbSchema, Vertex<TableMetadata>[] vxs, VertexRelationship<List<FKField>> vr) {
		// 左顶点
		Vertex<TableMetadata> left = vxs[vr.getFrom()];
		transformVertex(dbSchema, left);
		// 右顶点
		Vertex<TableMetadata> right = vxs[vr.getTo()];
		transformVertex(dbSchema, right);
		// 关系
		this.transformRelationship(dbSchema, vr);
	}
	// 转换关系
	public void transformRelationship(DbSchema dbSchema, VertexRelationship<List<FKField>> vr) {
		
	}
	
	// 获取表邻接数组位置
	private int locate(String tableName, Vertex<TableMetadata>[] vxs) {
		int l = 0;
		for(Vertex<TableMetadata> vx : vxs) {
			if(tableName.equals(vx.getData().getName()))
				break;
			l++;
		}
		return l;
			
	}
}
