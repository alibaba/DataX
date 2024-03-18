/**
 * %%
 * %%
 */
package com.leehom.arch.datax.plugin.rdb2graph.scanner;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.DbSchema;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.TableMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.constraint.NotNullConstraintMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.constraint.PKConstraintMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.constraint.UniqueConstraintMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.constraint.fk.FKConstraintMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.ds.BaseDataSourceBean;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.ds.DruidConnectionProperties;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.index.IndexMetadata;

import lombok.Data;

/**
 * @类名: AbstractDbScanner
 * @说明: 抽象库扫描, 
 *        获取schema，包括表，字段，索引，约束
 *
 * @author   leehom
 * @Date	 2022年1月7日 下午2:21:00
 * 修改记录：
 *
 * @see 	 
 */
@Data
public abstract class AbstractDbScanner {
	
	protected DruidConnectionProperties connProps;
	
	// 扫描数据库模式
	public abstract DbSchema scan(BaseDataSourceBean dsBean) throws Exception;
	// 扫描数据库
    protected abstract DbSchema doScanDb(DatabaseMetaData dbmd, ResultSet dbrs) throws SQLException;
    // 扫描数据表
    protected abstract TableMetadata doScanTable(String dbName, DatabaseMetaData metadata, ResultSet tablers) throws SQLException;
    // 扫描表的索引和约束
    // 主键/外键/非空/唯一
    protected abstract Optional<PKConstraintMetadata> doScanPK(String dbName, DatabaseMetaData dbmd, TableMetadata tbmd) throws SQLException;
    // 扫描外键，在扫描表后
    protected abstract Map<String, List<FKConstraintMetadata>> doScanFK(String dbName, DatabaseMetaData dbmd, List<TableMetadata> tbmds) throws SQLException;
    protected abstract List<NotNullConstraintMetadata> doScanNN(String dbName, DatabaseMetaData dbmd, TableMetadata tbmd) throws SQLException;
    protected abstract List<UniqueConstraintMetadata> doScanUnique(String dbName, DatabaseMetaData dbmd, TableMetadata tbmd) throws SQLException;
    // 索引
    protected abstract List<IndexMetadata> doScanIndex(String dbName, DatabaseMetaData dbmd, TableMetadata tbmd) throws SQLException;
    
    // 表扫描过滤
    protected boolean acceptTable(String tableName) {
    	List<String> tbs = connProps.getTbs();
    	if((tbs==null||tbs.size()==0)||tbs.contains(tableName))
    		return true;
    	return false;
    }
    // 查找外键引用表
    protected Optional<TableMetadata> findRefTable(String refName, List<TableMetadata> tbmds) {
		for(TableMetadata tbmd: tbmds) {
			if(refName.equals(tbmd.getName())) {
				return Optional.of(tbmd);
			}
		}
		return Optional.empty();
	}

}
