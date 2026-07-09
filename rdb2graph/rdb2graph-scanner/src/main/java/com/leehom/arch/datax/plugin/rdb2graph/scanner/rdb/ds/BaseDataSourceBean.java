/**
 * %项目描述%
 * %ver%     
 */
package com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.ds;

import javax.sql.DataSource;

import lombok.Data;

/**
 * @类名: BasicDataSource
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
public abstract class BaseDataSourceBean {
	
	public abstract DataSource toDataSource() throws Exception;
	
}
