package com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb;

import java.sql.JDBCType;

import lombok.Data;

/**
 * @类名: FieldMetadata
 * @说明: 字段元数据
 *        
 *
 * @author   leehom
 * @Date	 2022年1月7日 下午1:35:00
 * 修改记录：
 *
 * @see 	 
 */
@Data
public class FieldMetadata {
	
	private String name;
	/** 类型*/
	private JDBCType type;
	/** 长度*/
	private Integer length;
	/** 注释*/
	private String remark;

}
