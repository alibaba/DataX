/**
 * %%
 * %%
 */
package com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.constraint.fk;

import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.FieldMetadata;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @类名: FKField
 * @说明: 外键字段
 *
 * @author   leehom
 * @Date	 2022年4月16日 下午6:03:20
 * 修改记录：
 *
 * @see 	 
 */
@Data
@AllArgsConstructor
public class FKField {

	/** */
	private FieldMetadata field;
	/** 参考字段*/
	private FieldMetadata refField;
	
}
