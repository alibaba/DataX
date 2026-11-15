/**
 * %%
 * %%
 */
package com.leehom.arch.datax.plugin.rdb2graph.rdbms.reader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.util.Configuration;
import com.leehom.arch.datax.plugin.rdb2graph.common.RelRecord;
import com.leehom.arch.datax.plugin.rdb2graph.common.TableRecord;

/**
 * @类名: Phase
 * @说明: 读入阶段
 *
 * @author   leehom
 * @Date	 2020年9月3日 下午6:01:12s
 * 修改记录：
 *
 * @see 	 
 */
public enum Phase {

	TABLE, // 读入表
	REL // 表关系
	;
	// 
	public Record createRecord(Configuration readerSliceConfig) {
		if(this==Phase.TABLE) {
			String table = readerSliceConfig.getString(Key.TABLE);
			TableRecord tr = new TableRecord();
			tr.setTable(table);
			return tr;
		}
		else {
			String table = readerSliceConfig.getString(Constant.REL_FROM);
			String fk = readerSliceConfig.getString(Constant.REL_FK);
			RelRecord rr =  new RelRecord();
			rr.setFromTable(table);
			rr.setFk(fk);
			return rr;
		}
	}
}
