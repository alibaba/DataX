/**
 * %datax-graph%
 * %v1.0%
 */
package com.leehom.arch.datax.plugin.rdb2graph.common;

import com.alibaba.datax.core.transport.record.DefaultRecord;

/**
 * @类名: TableRecord
 * @说明: 表record
 *
 * @author   leehom
 * @Date	 2022年4月26日 下午7:31:18
 * 修改记录：
 *
 * @see 	 
 */
public class TableRecord extends DefaultRecord {

	/** 记录所属表*/
	private String table;

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}
	
	
}
