package com.leehom.arch.datax.plugin.rdb2graph.scanner.neo4j;

import java.util.List;

import lombok.Data;

/**
 * @类名: Neo4jSchema
 * @说明: neo4j图库模式
 *        
 *
 * @author   leehom
 * @Date	 2022年1月7日 下午1:35:00
 * 修改记录：
 *
 * @see 	 
 */
@Data
public class Neo4jSchema {
	
	/** 图库名称*/
	private String name;
	/** 图库描述*/
	private String remark;
	/** 图结构，邻接表*/
	private List<NodeMetadata> nodes;
	
}
