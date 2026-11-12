package com.leehom.arch.datax.plugin.rdb2graph.scanner.neo4j;

import com.leehom.arch.datax.plugin.rdb2graph.scanner.neo4j.type.Neo4jType;

import lombok.Data;

/**
 * @类名: PropertyMetadata
 * @说明: 属性元数据
 *        
 *
 * @author   leehom
 * @Date	 2022年1月7日 下午1:35:00
 * 修改记录：
 *
 * @see 	 
 */
@Data
public class PropertyMetadata {
	
	private String name;
	private Neo4jType type;
	private String remark;

}
