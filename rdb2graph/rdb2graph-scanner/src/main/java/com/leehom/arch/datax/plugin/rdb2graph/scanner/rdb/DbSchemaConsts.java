package com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb;


/**
 * @类名: DbSchemaConsts
 * @说明: 数据库元数据常量
 *
 * @author   leehom
 * @Date	 2013-4-10 下午2:31:41
 * @修改记录：
 *
 * @see 	 
 */
public interface DbSchemaConsts {
	
	/** */
	public static final int DB_NAME_INDEX = 1;
	public static final int DB_COMMENTS_INDEX = 9;
	
	public static final int TABLE_NAME_INDEX = 3;
	public static final int TABLE_COMMENTS_INDEX = 5;
	// 字段
	public static final int FEILD_NAME_INDEX = 4;
	public static final int FEILD_DATA_TYPE_INDEX = 5;
	public static final int FEILD_LENGTH_INDEX = 7;
	public static final int FEILD_NULLABLE_INDEX = 11;
	public static final int FEILD_REMARK_INDEX = 12;
	
	/** 主键约束*/
	public static final int PK_NAME_INDEX = 4;	
	
	/** 外键约束*/
	/** */
	public static final int FK_FIELD_COUNT = 14;
	/** 主表键名称*/
	public static final int FK_PK_FIELD_INDEX = 8;	
	/** 外键引用表名称*/
	public static final int FK_REF_TABLE_INDEX = 3;
	/** 外键引用字段名称*/
	public static final int FK_REF_FIELD_INDEX = 4;
	/** 外键名称*/
	public static final int FK_NAME_INDEX = 12;
	
	// 唯一约束
	/** 元数据结果集字段数*/
	public static final int UNIQUE_FIELD_COUNT = 14;
	public static final int UNIQUE_INDEX = 4;
	public static final int UNIQUE_FIELD_INDEX = 9;
	
	/** 索引*/
	public static final int INDEX_FIELD_COUNT = 14;
	public static final int INDEX_NAME_INDEX = 6;
	public static final int INDEX_FIELD_INDEX = 9;
	public static final int INDEX_TYPE_INDEX = 7;
	
	// 表类型，
	public static final String TABLE_TYPE = "TABLE";
	
    
}
