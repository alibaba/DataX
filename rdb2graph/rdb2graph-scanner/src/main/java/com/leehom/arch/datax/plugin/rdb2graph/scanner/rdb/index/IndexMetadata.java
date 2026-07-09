/**
 * %%
 * %%
 */
package com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.index;

import java.util.List;

import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.FieldMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.MultiFieldsTableComponent;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.TableMetadata;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @类名: IndexMetadata
 * @说明: 索引元数据，多字段
 *
 * @author   leehom
 * @Date	 2022年4月16日 下午6:03:20
 * 修改记录：
 *
 * @see 	 
 */
@Data
@EqualsAndHashCode(callSuper=false)
public class IndexMetadata  extends MultiFieldsTableComponent {

	public IndexMetadata(String name, TableMetadata table, List<FieldMetadata> fields, IndexType type) {
		super(table, fields);
		this.name = name;
		this.type = type;
	}
	
	private String name;
	private IndexType type;
	
}
