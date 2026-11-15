/**
 * %基础类%
 * %1.0%
 */
package com.leehom.arch.datax.plugin.rdb2graph.common.serializer;

/**
 * @类名: Serializer
 * @说明: 序列化器接口
 *
 * @author   leehom 
 * @Date	 2010-6-9 上午09:58:34
 * 修改记录：
 *
 * @see 	 
 */

public interface Serializer {

	public static final String PREFIX_CDATA    = "<![CDATA[";   
	public static final String SUFFIX_CDATA    = "]]>"; 
	
	/**
	 * @说明：序列化
	 *
	 * @author leehom
	 * @param obj 目标对象
	 * @throws SeriallizeException
	 * @return void
	 * 
	 * @异常：
	 */
	public byte[] Marshal(Object obj) throws SeriallizeException;
	
	/**
	 * @说明：反序列化
	 *
	 * @author leehong
	 * @param xml   XML字符串
	 *  typeAlias 类型关联，即XML与那个Class关联
	 * @return
	 * @throws SeriallizeException
	 * @return Object
	 * 
	 * @异常：
	 */
	public Object Unmarshal(byte[] xml) throws SeriallizeException;
}
