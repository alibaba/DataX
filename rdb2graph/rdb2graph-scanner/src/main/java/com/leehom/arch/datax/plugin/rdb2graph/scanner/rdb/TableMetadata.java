package com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb;

import java.util.List;
import java.util.Optional;

import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.constraint.NotNullConstraintMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.constraint.PKConstraintMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.constraint.UniqueConstraintMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.constraint.fk.FKConstraintMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.index.IndexMetadata;

import lombok.Data;
import lombok.Getter;

/**
 * @类名: TableMetadata
 * @说明: 数据库表元数据
 *        
 *
 * @author   leehom
 * @Date	 2022年1月7日 下午1:35:00
 * 修改记录：
 *
 * @see 	 
 */
@Data
public class TableMetadata {
	
	/** 表名称*/
	private String name;
	/** 注释*/
	private String remark;
	/** 字段元数据*/
	private List<FieldMetadata> fields;
	// 约束
	/** 主键*/
	private PKConstraintMetadata pk;
	/** 外键*/
	private List<FKConstraintMetadata> fks;
	/** 非空*/
	private List<NotNullConstraintMetadata> notNull;
	/** 唯一*/
	private List<UniqueConstraintMetadata> unique;
	// 索引
	private List<IndexMetadata> indexes;
	/** 
	 * 是否连接表
	 * link->t1->t2
	 */
	private boolean isLinkTable;
	/** 连接表有效，连接关系起点外键*/
	private FKConstraintMetadata  linkFrom;
	
	public Optional<FieldMetadata> findField(String name) {
		for(FieldMetadata fmd : fields) {
			if(fmd.getName().equals(name))
				return Optional.of(fmd);
		}
		return Optional.empty();
	}
	
	public FKConstraintMetadata findFk(String name) {
		for(FKConstraintMetadata fkmd : fks) {
			if(fkmd.getFkName().equals(name))
				return fkmd;
		}
		return null;
	}
	
}
