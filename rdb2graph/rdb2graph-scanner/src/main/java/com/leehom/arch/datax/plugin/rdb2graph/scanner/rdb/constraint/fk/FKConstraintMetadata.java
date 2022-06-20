/**
 * %%
 * %%
 */
package com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.constraint.fk;

import java.util.List;

import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.FieldMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.MultiFieldsTableComponent;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.TableMetadata;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @类名: FKConstraintMetadata
 * @说明: 外键约束元数据
 *
 * @author   leehom
 * @Date	 2022年4月16日 下午6:03:20
 * 修改记录：
 *
 * @see 	 
 */
@Data
@EqualsAndHashCode(callSuper=false)
public class FKConstraintMetadata extends MultiFieldsTableComponent {
	
	public FKConstraintMetadata(String name, TableMetadata table, TableMetadata refTable, List<FieldMetadata> fields, List<FKField> fkFields) {
		super(table, fields);
		this.fkName = name;
		this.refTable = refTable;
		this.fkFields = fkFields;
	}
	/** 外键名称*/
	private String fkName;
	/** */
	private TableMetadata refTable;
	/** 字段元数据*/
	private List<FKField> fkFields;
	
}
