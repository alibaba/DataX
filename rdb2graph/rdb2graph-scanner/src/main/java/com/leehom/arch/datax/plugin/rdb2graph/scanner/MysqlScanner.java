/**
 * %%
 * %%
 */
package com.leehom.arch.datax.plugin.rdb2graph.scanner;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.sql.DataSource;

import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.DbSchema;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.DbSchemaConsts;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.FieldMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.TableMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.constraint.NotNullConstraintMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.constraint.PKConstraintMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.constraint.UniqueConstraintMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.constraint.fk.FKConstraintMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.constraint.fk.FKField;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.ds.BaseDataSourceBean;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.index.IndexMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.index.IndexType;

/**
 * @类名: MysqlScanner
 * @说明: 数据库扫描, 
 *        表，字段，索引，约束
 *
 * @author   leehom
 * @Date	 2022年1月7日 下午2:21:00
 * 修改记录：
 *
 * @see 	 
 */
public class MysqlScanner extends AbstractDbScanner {
	
	// 扫描数据库模式
	public DbSchema scan(BaseDataSourceBean dsBean) throws Exception {
		// 构建数据源，获取数据库连接
		DataSource ds = dsBean.toDataSource();
		Connection conn = ds.getConnection();
		//
		String db = connProps.getDb();
		// db元数据
		DatabaseMetaData dbmd = conn.getMetaData();   
		// 模式结果集
		ResultSet schemars = dbmd.getCatalogs();
		//
		while (schemars.next()) {
			// 扫描数据库
			String dbName = schemars.getString(DbSchemaConsts.DB_NAME_INDEX);
			if(db.equals(dbName)) {
				DbSchema schema = this.doScanDb(dbmd, schemars);
				schemars.close();
				return schema;
			}
		}
		// TODO 使用统一异常设计
		throw new Exception("no db found!");
		
	}
	
	// 扫描数据库
	@Override
    public DbSchema doScanDb(DatabaseMetaData metadata, ResultSet dbrs) throws SQLException {
    	//
		DbSchema dbmd = new DbSchema();
    	//
    	String dbName = dbrs.getString(DbSchemaConsts.DB_NAME_INDEX);
    	dbmd.setName(dbName);
		// 表元数据, 只获取表
        ResultSet tablers = metadata.getTables(dbName, null, null, new String[] {DbSchemaConsts.TABLE_TYPE});   
        List<TableMetadata> tbmds = new ArrayList<>();
        while (tablers.next()) {   
			// 扫描表
        	String tbName = tablers.getString(DbSchemaConsts.TABLE_NAME_INDEX);
        	if(!this.acceptTable(tbName))
        		continue;
			TableMetadata tbmd = this.doScanTable(dbName, metadata, tablers);
			tbmds.add(tbmd);

		}
        dbmd.setTables(tbmds);
        tablers.close();  
        // 表扫描完，扫描外键, 外键需引用其他表 
        doScanFK(dbName, metadata, tbmds);
        //
        return dbmd;
    }
    
    // 扫描数据表
	@Override
    public TableMetadata doScanTable(String dbName, DatabaseMetaData metadata, ResultSet tablers) throws SQLException {
    	TableMetadata tbmd = new TableMetadata();
    	String tbName = tablers.getString(DbSchemaConsts.TABLE_NAME_INDEX);
    	tbmd.setName(tbName);
    	// 扫描表字段
    	List<FieldMetadata> fdmds = new ArrayList<>();
    	ResultSet feildrs = metadata.getColumns(dbName, null, tbName, null);
    	// 用于调试
    	// ResultSetMetaData fmd = feildrs.getMetaData();
        while (feildrs.next()) {   
        	FieldMetadata fdmd = new FieldMetadata();
        	// 字段名称/类型/注释
        	String fieldName = feildrs.getString(DbSchemaConsts.FEILD_NAME_INDEX);
        	fdmd.setName(fieldName);
        	int type = feildrs.getInt(DbSchemaConsts.FEILD_DATA_TYPE_INDEX);
        	fdmd.setType(JDBCType.valueOf(type));
        	int length = feildrs.getInt(DbSchemaConsts.FEILD_LENGTH_INDEX);
        	fdmd.setLength(length);
        	String remark = feildrs.getString(DbSchemaConsts.FEILD_REMARK_INDEX);
        	fdmd.setRemark(remark);
            fdmds.add(fdmd);
        }
        feildrs.close();
        tbmd.setFields(fdmds);
        // 扫描主键
        doScanPK(dbName, metadata, tbmd);
        // 非空约束
        doScanNN(dbName, metadata, tbmd);
        // 唯一约束
        doScanUnique(dbName, metadata, tbmd);
        // 索引
        doScanIndex(dbName, metadata, tbmd);
        // 
        return tbmd;
    }
	
	// 扫描约束，主键；外键；非空；唯一
	@Override
	protected Optional<PKConstraintMetadata> doScanPK(String dbName, DatabaseMetaData dbmd, TableMetadata tbmd) throws SQLException {
		// 主键结果集
		ResultSet pkrs = dbmd.getPrimaryKeys(dbName, null, tbmd.getName());
		// 主键字段
		List<FieldMetadata> pkfs = new ArrayList<>();
		// 主键元数据
		// ResultSetMetaData pkmd = pkrs.getMetaData();
		while(pkrs.next()){
			String pkFn = pkrs.getString(DbSchemaConsts.PK_NAME_INDEX);
			Optional<FieldMetadata> op = tbmd.findField(pkFn);
			if(op.isPresent())
				pkfs.add(op.get());
		}
		// 无主键
		if(pkfs.size()==0) {
			return Optional.empty();
		}
		tbmd.setPk(new PKConstraintMetadata(tbmd, pkfs));
		return Optional.of(tbmd.getPk());
	}

	@Override
	protected Map<String, List<FKConstraintMetadata>> doScanFK(String dbName, DatabaseMetaData dbmd, List<TableMetadata> tbmds) throws SQLException {
		Map<String, List<FKConstraintMetadata>> r = new HashMap<>();
		// 循环扫描表
		for(TableMetadata tbmd: tbmds) {
			List<FKConstraintMetadata> fks = scanFKTable(dbName, dbmd, tbmd, tbmds);
			r.put(tbmd.getName(), fks);
		}
		return r;
	}
	
	/**
	 * @说明：扫描表外键索引
	 *        一个表可有多个外键，一个外键可有多个字段
	 *
	 * @author leehom
	 * @param dbName 
	 * @param dbmd
	 * @param tbmd  扫描目标表
	 * @param tbmds 所有表
	 * @return
	 * @throws SQLException
	 * 
	 */
	private List<FKConstraintMetadata> scanFKTable(String dbName, DatabaseMetaData dbmd, TableMetadata tbmd, List<TableMetadata> tbmds) throws SQLException {
		// 外键元数据结果集
		ResultSet fkrs = dbmd.getImportedKeys(dbName, dbName, tbmd.getName());
		// 主键元数据
		// ResultSetMetaData fkmd = fkrs.getMetaData();
		// 
		List<FKConstraintMetadata> fkcmds = new ArrayList<>();
		// key: 外键名称
		Map<String, FKConstraintMetadata> fkcmdMap = new HashMap<>();
		while(fkrs.next()){
			// 外键名称
			String fkName = fkrs.getString(DbSchemaConsts.FK_NAME_INDEX);
			// 外键引用表
			String fkTableName = fkrs.getString(DbSchemaConsts.FK_REF_TABLE_INDEX);
			TableMetadata refTable = this.findRefTable(fkTableName, tbmds).get();
			// 主表外键字段
			String pfn = fkrs.getString(DbSchemaConsts.FK_PK_FIELD_INDEX);
			FieldMetadata pf = tbmd.findField(pfn).get();
			//
			String ffn = fkrs.getString(DbSchemaConsts.FK_REF_FIELD_INDEX);
			FieldMetadata ff = refTable.findField(ffn).get();
			//
			FKField fkf = new FKField(pf, ff);
			// 外键存在，即，多字段外键
			if(fkcmdMap.containsKey(fkName)) {
				FKConstraintMetadata fkcmd = fkcmdMap.get(fkName);
				fkcmd.getFields().add(pf);
				fkcmd.getFkFields().add(fkf);
			} else {
				List<FieldMetadata> fields = new ArrayList<>(); 
				fields.add(pf);
				List<FKField> fkFields = new ArrayList<>();
				fkFields.add(fkf);
				FKConstraintMetadata fkcmd = new FKConstraintMetadata(fkName, tbmd, refTable, fields, fkFields); 
				fkcmds.add(fkcmd);
				fkcmdMap.put(fkName, fkcmd);
			}
		}
		// 外键放入表
		tbmd.setFks(fkcmds);
		return fkcmds;
	}
	
	@Override
	protected List<NotNullConstraintMetadata> doScanNN(String dbName, DatabaseMetaData dbmd, TableMetadata tbmd) throws SQLException {
		// 
    	List<NotNullConstraintMetadata> nns = new ArrayList<>();
    	// 字段元数据
    	ResultSet feildrs = dbmd.getColumns(dbName, null, tbmd.getName(), null);
        while (feildrs.next()) {   
        	// 
        	String fieldName = feildrs.getString(DbSchemaConsts.FEILD_NAME_INDEX);
        	int nullable = feildrs.getInt(DbSchemaConsts.FEILD_NULLABLE_INDEX);
        	// nullable==0 0为法false，即不能为空
        	if(nullable==0) {
	        	Optional<FieldMetadata> op = tbmd.findField(fieldName);
	        	if(op.isPresent()) {
	        		nns.add(new NotNullConstraintMetadata(tbmd, op.get()));
	        	}
        	}
        }
        feildrs.close();
        tbmd.setNotNull(nns);
        return nns;
	}

	// 唯一约束
	@Override
	protected List<UniqueConstraintMetadata> doScanUnique(String dbName, DatabaseMetaData dbmd, TableMetadata tbmd) throws SQLException {
		List<UniqueConstraintMetadata> uqmds = new ArrayList<>();
		//
		ResultSet indexrs = dbmd.getIndexInfo(dbName, null, tbmd.getName(), false, false);
		// ResultSetMetaData indexmd = indexrs.getMetaData();
		//
		Map<String, UniqueConstraintMetadata> uqmdMap = new HashMap<>();
		while (indexrs.next()) {
			// 名称，与索引共用
			String unqName = indexrs.getString(DbSchemaConsts.INDEX_NAME_INDEX);
			// nonUnique=true, 即不唯一
			boolean nonUnique = indexrs.getBoolean(DbSchemaConsts.UNIQUE_INDEX);
			if (nonUnique) 
				continue;
			String fn = indexrs.getString(DbSchemaConsts.UNIQUE_FIELD_INDEX);
			FieldMetadata field = tbmd.findField(fn).get();
			if(uqmdMap.containsKey(unqName)) {
				UniqueConstraintMetadata uqmd = uqmdMap.get(unqName);
				uqmd.getFields().add(field);
			} else {
				List<FieldMetadata> fields = new ArrayList<>(); 
				fields.add(field);
				UniqueConstraintMetadata uqmd = new UniqueConstraintMetadata(unqName, tbmd, fields); 
				uqmds.add(uqmd);
				uqmdMap.put(unqName, uqmd);
			}
		}
		tbmd.setUnique(uqmds);
		return uqmds;
	}

	// 索引
	@Override
	protected List<IndexMetadata> doScanIndex(String dbName, DatabaseMetaData dbmd, TableMetadata tbmd) throws SQLException {
		List<IndexMetadata> indexmds = new ArrayList<>();
		//
		ResultSet indexrs = dbmd.getIndexInfo(dbName, null, tbmd.getName(), false, false);
		// 
		// ResultSetMetaData indexrsmd = indexrs.getMetaData();
		// key: 索引名称
		Map<String, IndexMetadata> indexmdMap = new HashMap<>();
		while (indexrs.next()) {
			//
			String indexName = indexrs.getString(DbSchemaConsts.INDEX_NAME_INDEX);
			// 非空索引不处理，以非空约束处理
			boolean nonUnique = indexrs.getBoolean(DbSchemaConsts.UNIQUE_INDEX);
			if (!nonUnique) 
				continue;
			// 索引字段
			String fn = indexrs.getString(DbSchemaConsts.INDEX_FIELD_INDEX);
			FieldMetadata field = tbmd.findField(fn).get();
			// 已有索引存储，多字段索引
			if(indexmdMap.containsKey(indexName)) {
				IndexMetadata indexmd = indexmdMap.get(indexName);
				indexmd.getFields().add(field);
			} else {
				List<FieldMetadata> fields = new ArrayList<>(); 
				fields.add(field);
				// 索引类型
				int indexType = indexrs.getInt(DbSchemaConsts.INDEX_TYPE_INDEX);
				IndexMetadata indexmd = new IndexMetadata(indexName, tbmd, fields, IndexType.values()[indexType]);
				indexmds.add(indexmd);
				indexmdMap.put(indexName, indexmd);
			}
		}
		tbmd.setIndexes(indexmds);
		return indexmds;
	}

}
