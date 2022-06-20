/**
 * %项目描述%
 * %ver%     
 */
package com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.ds;

import lombok.Data;

/**
 * @类名: DruidConnectionProperties
 * @说明: druid连接池属性
 *          
 *
 * @author: leehom
 * @Date	2018年10月22日 下午4:39:19
 * @修改记录：
 *
 * @see
 * 
 *     
 */
@Data
public class BaseConnectionProperties {

	private Long id;
	private String name;
	/** jdbc连接url*/
	private String url;
	/** 驱动类类型*/
	private String driverClass;
	/** 账号&密码*/
	private String userName;
	private String password;
	private String remark;
	
}
