/**
 * %%
 * %%
 */
package com.leehom.arch.datax.plugin.rdb2graph.writer.neo4jwriter;

/**
 * @类名: WriteMode
 * @说明: 写入模式
 *
 * @author   leehom
 * @Date	 2020年9月3日 下午6:01:12s
 * 修改记录：
 *
 * @see 	 
 */
public enum WriteMode {

	// CLEAR_BEFORE_INSERT, // 清除
	INSERT, // 直接写入
	INSERT_NOT_EXIST,
	REPALCE, // 每次删除后插入
	UPDATE // 更新模式, 不存在插入
	;

}
