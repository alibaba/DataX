/**
 * %%
 * %%
 */
package com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @类名: TableComponent
 * @说明: 表功能构件
 *        约束，保证数据完整性
 *        索引，加快数据搜索
 *        
 *
 * @author   leehom
 * @Date	 2022年4月16日 下午6:03:20
 * 修改记录：
 *
 * @see 	 
 */
@Data
@AllArgsConstructor
public abstract class TableComponent {

	/** 所属表*/
	private TableMetadata tbmd;
}
