/**
 * %%
 * %%
 */
package com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @类名: SingleFieldDbSchemaComponent
 * @说明: 单字段数据库构件
 *        
 *
 * @author   leehom
 * @Date	 2022年4月16日 下午6:03:20
 * 修改记录：
 *
 * @see 	 
 */
@Data
@EqualsAndHashCode(callSuper=false)
public class SingleFieldTableComponent extends TableComponent {
		
	public SingleFieldTableComponent(TableMetadata tbmd, FieldMetadata field) {
		super(tbmd);
		this.field = field;
	}

	/** 约束作用字段*/
	private FieldMetadata field;
	
}
