/**
 * %项目描述%
 * %ver%     
 */
package com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.ds;

import javax.sql.DataSource;

import com.alibaba.druid.pool.DruidDataSource;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @类名: DataSource
 * @说明: 数据源
 *          
 *
 * @author: leehom
 * @Date	2018年10月22日 下午4:35:30
 * @修改记录：
 *
 * @see
 * 
 *     
 */
@Data
@EqualsAndHashCode(callSuper=false)
public class DruidDataSourceBean extends BaseDataSourceBean {
	
	/** 连接属性*/
	private DruidConnectionProperties connProps;

	@Override
	public DataSource toDataSource() throws Exception {
		DruidDataSource druidDataSource = new DruidDataSource();
        druidDataSource.setDriverClassName(connProps.getDriverClass());
        druidDataSource.setUrl(connProps.getUrl());
        druidDataSource.setUsername(connProps.getUserName());
        druidDataSource.setPassword(connProps.getPassword());
        druidDataSource.setInitialSize(connProps.getInitialSize());
        druidDataSource.setMinIdle(connProps.getMinIdle());
        // druidDataSource.setMaxActive(Integer.parseInt(propertyResolver.getProperty("maxActive")));
        druidDataSource.setMaxWait(connProps.getMaxWait());
        druidDataSource.setTimeBetweenEvictionRunsMillis(connProps.getTimeBetweenEvictionRunsMillis());
        druidDataSource.setMinEvictableIdleTimeMillis(connProps.getMinEvictableIdleTimeMillis());
        druidDataSource.setValidationQuery(connProps.getValidationQuery());
        druidDataSource.setTestWhileIdle(connProps.isTestWhileIdle());
        druidDataSource.setTestOnBorrow(connProps.isTestOnBorrow());
        druidDataSource.setTestOnReturn(connProps.isTestOnReturn());
        druidDataSource.setPoolPreparedStatements(connProps.isPoolPreparedStatements());
        druidDataSource.setMaxPoolPreparedStatementPerConnectionSize(connProps.getMaxOpenPreparedStatements());
        druidDataSource.setKeepAlive(true);
        druidDataSource.init();
        return druidDataSource;

	}
	
}
