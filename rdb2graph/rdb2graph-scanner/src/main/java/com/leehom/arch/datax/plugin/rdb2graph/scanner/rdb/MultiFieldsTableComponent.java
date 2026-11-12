/**
 * %%
 * %%
 */
package com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb;

import java.util.List;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @类名: MultiFieldsDbSchemaComponent
 * @说明: 多字段数据库构件
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
public class MultiFieldsTableComponent extends TableComponent {

	public MultiFieldsTableComponent(TableMetadata tbmd, List<FieldMetadata> fields) {
		super(tbmd);
		this.fields = fields;
	}
	
	/** 约束作用字段*/
	private List<FieldMetadata> fields;
	
}
