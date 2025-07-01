/**
 * %项目描述%
 * %ver%     
 */
package com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.ds;

import java.util.List;

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
public class DruidConnectionProperties {
	private Long id;
	private String name;
	/** 目标数据库*/
	private String db;
	/** 表，白名单*/
	private List<String> tbs;
	/** jdbc连接url*/
	private String url;
	/** 驱动类类型*/
	private String driverClass;
	/** 账号&密码*/
	private String userName;
	private String password;
	private String remark;
	/** 初始连接数*/
	private int initialSize = 1;
	/** 最大请求等待时间*/
	private int maxWait = 60000;
	/** 最少空闲连接数*/
	private int minIdle = 1;
	/** 检查空闲连接频率*/
	private int timeBetweenEvictionRunsMillis = 60000;
	/** 最小空闲时间, 10分钟*/
	private int minEvictableIdleTimeMillis = 600000;
	/** 最大空闲时间, 60分钟*/
	private int maxEvictableIdleTimeMillis = 3600000;
	/** */
	private String validationQuery = "select 'x'";
	/** 有效性测试*/
	private boolean testWhileIdle = true;
	/** 有效性测试*/
	private boolean testOnBorrow = false;
	/** 有效性测试*/
	private boolean testOnReturn = false;
	/** 缓存sql*/
	private boolean poolPreparedStatements = true;
	/** 最多缓存SQL数*/
	private int maxOpenPreparedStatements = 20;
	
}
