/**
 * %%
 * %%
 */
package com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.constraint;

import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.FieldMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.SingleFieldTableComponent;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.TableMetadata;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @类名: NotNullConstraintMetadata
 * @说明: 非空约束元数据
 *
 * @author   leehom
 * @Date	 2022年4月16日 下午6:03:20
 * 修改记录：
 *
 * @see 	 
 */
@Data
@EqualsAndHashCode(callSuper=false)
public class NotNullConstraintMetadata extends SingleFieldTableComponent {

	public static final String NN_NAME = "_NN";
	
	public NotNullConstraintMetadata(TableMetadata tbmd, FieldMetadata field) {
		super(tbmd, field);
		this.nnName = field.getName().toUpperCase()+NN_NAME;
	}
	
	private String nnName;

}
