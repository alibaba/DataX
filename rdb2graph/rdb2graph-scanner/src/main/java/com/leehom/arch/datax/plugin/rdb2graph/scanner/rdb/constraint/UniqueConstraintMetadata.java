/**
 * %%
 * %%
 */
package com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.constraint;

import java.util.List;

import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.FieldMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.MultiFieldsTableComponent;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.TableMetadata;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @类名: UniqueConstraintMetadata
 * @说明: 唯一约束元数据, 多字段
 *
 * @author   leehom
 * @Date	 2022年4月16日 下午6:03:20
 * 修改记录：
 *
 * @see 	 
 */
@Data
@EqualsAndHashCode(callSuper=false)
public class UniqueConstraintMetadata extends MultiFieldsTableComponent {

	public UniqueConstraintMetadata(String unqName, TableMetadata table, List<FieldMetadata> fields) {
		super(table, fields);
		this.unqName = unqName;
	}
	
	private String unqName;
	
}
