package com.leehom.arch.datax.plugin.rdb2graph.scanner.neo4j;

import java.util.List;

import lombok.Data;

/**
 * @类名: NodeMetadata
 * @说明: 节点元数据
 *        
 *
 * @author   leehom
 * @Date	 2022年1月7日 下午1:35:00
 * 修改记录：
 *
 * @see 	 
 */
@Data
public class NodeMetadata {
	
	private String label;
	private String remark;
	private List<RelationshipMetadata> relationships;

}
