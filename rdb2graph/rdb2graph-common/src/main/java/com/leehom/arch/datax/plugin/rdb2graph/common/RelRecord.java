/**
 * %datax-graph%
 * %v1.0%
 */
package com.leehom.arch.datax.plugin.rdb2graph.common;

import com.alibaba.datax.core.transport.record.DefaultRecord;

/**
 * @类名: RelRecord
 * @说明: 关系record
 *
 * @author   leehom
 * @Date	 2022年4月26日 下午7:31:18
 * 修改记录：
 *
 * @see 	 
 */
public class RelRecord extends DefaultRecord {

	/** 关系起始表或者连接表*/
	private String fromTable;
	/**
	 * 若连接表，fk则为连接的起点的关联表
	 * 若为一般表，fk则为连接的外键，表可有多个外键 
	 */
	private String fk;
	
	public String getFk() {
		return fk;
	}
	public void setFk(String fk) {
		this.fk = fk;
	}
	public String getFromTable() {
		return fromTable;
	}
	public void setFromTable(String fromTable) {
		this.fromTable = fromTable;
	}
   
	
}
