package com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb;

import java.util.ArrayList;
import java.util.List;

import com.leehom.arch.datax.plugin.rdb2graph.scanner.graph.Graph;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.graph.VertexRelationship;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.graph.Graph.Vertex;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.constraint.fk.FKConstraintMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.constraint.fk.FKField;

import lombok.Data;

/**
 * @类名: DbSchema
 * @说明: 数据库模式
 *        
 *
 * @author   leehom
 * @Date	 2022年1月7日 下午1:35:00
 * 修改记录：
 *
 * @see 	 
 */
@Data
public class DbSchema {
	
	/** */
	private String name;
	/** 注释*/
	private String remark;
	/** 数据库表*/
	private List<TableMetadata> tables;
	
	/**
	 * @说明：设置/去除表为连接表
	 *
	 * @author leehom
	 * @param table
	 * @param linkFromFK  
	 * 
	 */
	public void setLinkTable(TableMetadata table, FKConstraintMetadata linkFromFK) {
		// 验证是否符合连接表， node->edge->edge
		// TODO 统一异常规范
		if(table.getFks()==null||!(table.getFks().size()==2)) {
			throw new RuntimeException("不符合连接表形态，node->edge->edge");
		}
		table.setLinkTable(true);
		table.setLinkFrom(linkFromFK);
	}
	
	public void unSetLinkTable(TableMetadata table) {
		table.setLinkTable(false);
		table.setLinkFrom(null);
	}
	
	// 计算表连接图
	@SuppressWarnings("unchecked")
	public Graph buildTableGraph() {
		Vertex<String>[] vxs = new Vertex[tables.size()];
		// 构建顶点
		for(int i=0;i<vxs.length;i++) {
			vxs[i] = new Vertex<>(tables.get(i).getName());
		}
		// 构建关系
		List<VertexRelationship<List<FKField>>> vrs = new ArrayList<>();
		for(int i=0;i<vxs.length;i++) {
			TableMetadata table= tables.get(i);
			List<FKConstraintMetadata> fks = table.getFks();
			int from = i;
			for(FKConstraintMetadata fk : fks) {
				// 
				int to = locate(fk.getRefTable().getName(), vxs);
				// 构建关系
				VertexRelationship<List<FKField>> vr = new VertexRelationship<List<FKField>>(from, to, fk.getFkFields());
				vrs.add(vr);
			}
			
		}
		// 构建表连接图
		return Graph.buildGraph(vxs, vrs.toArray(new VertexRelationship[0]));
	}
	
	// 获取表邻接数组位置
	private int locate(String tableName, Vertex<String>[] vxs) {
		int l = 0;
		for(Vertex<String> vx : vxs) {
			if(tableName.equals(vx.getData()))
				break;
			l++;
		}
		return l;
			
	}
	
	public TableMetadata findTable(String name) {
		for(TableMetadata t : tables) {
			if(t.getName().equals(name)) {
				return t;
			}
		}
		return null;
	}

}
