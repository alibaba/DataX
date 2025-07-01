package com.leehom.arch.datax.plugin.rdb2graph.scanner.graph;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @类名: VertexRelationship
 * @说明: 顶点关系
 *        
 *
 * @author   leehom
 * @Date	 2022年1月7日 下午1:35:00
 * 修改记录：
 *
 * @see 	 
 */
@Data
@AllArgsConstructor
public class VertexRelationship<V> {
	
	public VertexRelationship(int from, int to) {
		this.from = from;
		this.to = to;
	}
	private int from;
	private int to;
	private V data;

}
