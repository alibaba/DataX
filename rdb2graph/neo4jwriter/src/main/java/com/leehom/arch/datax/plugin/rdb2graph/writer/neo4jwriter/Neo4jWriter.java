package com.leehom.arch.datax.plugin.rdb2graph.writer.neo4jwriter;

import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.sql.JDBCType;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.reflect.MethodUtils;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Query;
import org.neo4j.driver.exceptions.Neo4jException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Column.Type;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.leehom.arch.datax.plugin.rdb2graph.common.ByteAndStreamUtils;
import com.leehom.arch.datax.plugin.rdb2graph.common.ResourceLoaderUtil;
import com.leehom.arch.datax.plugin.rdb2graph.common.serializer.Serializer;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.config.ScannerSerializerConfig;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.neo4j.ds.Neo4jDao;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.neo4j.ds.Neo4jQueryPattern;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.neo4j.ds.ParamsUtils;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.DbSchema;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.FieldMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.TableMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.constraint.fk.FKConstraintMetadata;
import com.leehom.arch.datax.plugin.rdb2graph.scanner.rdb.constraint.fk.FKField;

/**
 * @类名: Neo4jWriter
 * @说明: neo4j写入，支持两个阶段
 *        
 *
 * @author   leehom
 * @Date	 2022年4月21日 下午11:14:32
 * 修改记录：
 *
 * TODO
 *   1. 支持写入模式，@WriteMode replace/update
 *   
 * @see 	 
 */
public class Neo4jWriter extends Writer {

    public static class Job extends Writer.Job {
    	
        private static final Logger log = LoggerFactory.getLogger(Job.class);

        private Configuration conf = null;

        // 初始化环境和配置，为客户端准备环境
        @Override
        public void init() {
            this.conf = super.getPluginJobConf();
        }

        // 准备
        // 这里可做schema转换
        @Override
        public void prepare() {
        	//
        }

        // 目标库只有一个，分片相当于并行写入，克隆即可
        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> configurations = new ArrayList<Configuration>(mandatoryNumber);
            for (int i = 0; i < mandatoryNumber; i++) {
                configurations.add(conf);
            }
            return configurations;
        }

        // 后处理
        @Override
        public void post() {
        	// nothing
        }

        // 释放资源
        @Override
        public void destroy() {
        	// nothing
        }
    }

    public static class Task extends Writer.Task {

        private static final Logger log = LoggerFactory.getLogger(Job.class);

        private Configuration conf;
        private Neo4jDao client = null; 
        private int batchSize; // 支持批量
        private DbSchema rdbSchema; //
        private WriteMode writeMode; // 写入模式，
        private Serializer ser; // 

        @Override
        public void init() {
        	// 
            this.conf = super.getPluginJobConf();
            // 初始化 Neo4jDao
            String uri = Key.uri(conf);
            String un = Key.userName(conf);
            String pw = Key.password(conf);
            String db = Key.database(conf);
            Driver driver = GraphDatabase.driver(uri, AuthTokens.basic(un, pw));
            client = new Neo4jDao();
            client.setDriver(driver);
            client.setDatabase(db);
            // 初始化数据库schema序列化器
            ser = ScannerSerializerConfig.rdbSchemaXmlSerializer();
            // 写入批量
            batchSize = Key.batchSize(conf);
        }

        @Override
        public void prepare() {
        	init(); // 作业容器并没有调用init方法
            // 载入关系数据库schema 
        	String schemaUri = Key.schemaUri(conf);
        	InputStream is;
			try {
				is = ResourceLoaderUtil.getResourceStream(schemaUri);
	    		byte[] bytes = ByteAndStreamUtils.StreamToBytes(is);
	    		rdbSchema = (DbSchema)ser.Unmarshal(bytes);
			} catch (Exception e) {
	        	DataXException.asDataXException(Neo4jWriterErrorCode.ERROR_LOAD_RDBSCHEMA, e);
			} 

        }

        // 写入, 两个场景
        // 1. 表记录 TableRecord 2. 关系记录 RelRecord
        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            List<Record> writerBuffer = new ArrayList<Record>(this.batchSize);
            Record record = null;
            // 交换器(Exchanger)接收到TerminateRecord返回null
            while ((record = recordReceiver.getFromReader()) != null) {
                writerBuffer.add(record);
                if (writerBuffer.size() >= this.batchSize) {
                	this.doWrite(writerBuffer);
                    writerBuffer.clear();
                }
            }
            if (!writerBuffer.isEmpty()) {
            	this.doWrite(writerBuffer);
                writerBuffer.clear();
            }

        }
        
        // 写入分流 节点 / 关系
		private void doWrite(List<Record> writerBuffer) {
			Record record = writerBuffer.get(0);
			try {
				if ("TableRecord".equals(record.getClass().getSimpleName())) {
					doBatchWriteNode(writerBuffer);
					return;
				}
				if ("RelRecord".equals(record.getClass().getSimpleName())) {
					doBatchWriteRel(writerBuffer);
					return;
				}
				// 系统sleep
				try {
					Thread.sleep(300);
				} catch (InterruptedException e) {

				}
			} catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
				// 理论上不会发送，只记录
				log.error(e.getMessage());

			} catch (DataXException e) {
				// 字段类型不合法，记录，其他抛出
				log.error(e.getMessage());

			} catch (Neo4jException e) {
				// neo4j异常
				throw DataXException.asDataXException(Neo4jWriterErrorCode.WRONG_NEO4j_CLIENT,
						Neo4jWriterErrorCode.WRONG_NEO4j_CLIENT.getDescription(), e);
			}

		}

        // 批量写入
        private void doBatchWriteNode(final List<Record> writerBuffer) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        	List<Query> queries = new ArrayList<>();
            for (Record record : writerBuffer) {
        		String tn = (String) MethodUtils.invokeMethod(record, "getTable");
				TableMetadata tbmd = rdbSchema.findTable(tn);
				if (tbmd.isLinkTable())
					continue;
				// 节点属性
				Map<String, Object> props = fillProperties(tbmd, record);
				Query q = calcNodeWriteQuery(tbmd, props);
				queries.add(q);
			}
            client.reTryRunInTransaction(queries, 5);
            	
        }
        
		private void doBatchWriteRel(final List<Record> writerBuffer)
				throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
			List<Query> queries = new ArrayList<>();
			for (Record record : writerBuffer) {
				// 表名称
				String tn;
				String fk;

				tn = (String) MethodUtils.invokeMethod(record, "getFromTable");
				fk = (String) MethodUtils.invokeMethod(record, "getFk");
				TableMetadata tbmd = rdbSchema.findTable(tn);
				// 关系起始表，外键
				TableMetadata from;
				TableMetadata to;
				String fkName;
				Map<String, Object> props;
				// 连接表
				if (tbmd.isLinkTable()) {
					// 作为起点的外键，连接表有且仅有2外键，其中一个作为起点，另一个为关系终点
					FKConstraintMetadata fromFkmd = tbmd.getLinkFrom();
					from = fromFkmd.getRefTable();
					//
					FKConstraintMetadata toFkmd = tbmd.getFks().get(0).getFkName().equals(fromFkmd.getFkName())
							? tbmd.getFks().get(1)
							: tbmd.getFks().get(0);
					to = toFkmd.getRefTable();
					fkName = tbmd.getName();
					props = fillProperties(tbmd, record);
					Query q = calcLinkRelWriteQuery(from, to, tbmd, fromFkmd, toFkmd, props);
					queries.add(q);	
				} else {
					from = tbmd;
					FKConstraintMetadata fkmd = from.findFk(fk);
					to = fkmd.getRefTable();
					fkName = fkmd.getFkName();
					props = new HashMap<String, Object>();
					Query q = calcRelWriteQuery(from, to, record, fkName, props);
					queries.add(q);	
				}
				// 构建查询

			}
			client.reTryRunInTransaction(queries, 5);
		}
        
        // 构建写入 query, 写入模式  
		// @WriteMode
        private Query calcNodeWriteQuery(TableMetadata tbmd, Map<String, Object> props) {
        	// insert
        	String propsStr = ParamsUtils.params2String(props);
        	String cql = MessageFormat.format(Neo4jQueryPattern.CREATE_NODE, tbmd.getName(), propsStr);
        	return new Query(cql);
        }
        
        private Query calcRelWriteQuery(TableMetadata from, TableMetadata to, Record record, String fkName, Map<String, Object> props) {
        	// 连接属性
        	String propsStr = ParamsUtils.params2String(props);
        	// 节点过滤条件，使用主键
        	String nodeWherePattern = "{0}.{1} = {2}";
        	List<String> nodeWhereItems = new ArrayList<>();
        	//
        	List<FieldMetadata> pkfs = from.getPk().getFields();
        	FKConstraintMetadata fk = from.findFk(fkName);
        	// 
        	List<FKField> fkfs = fk.getFkFields();
        	// from节点
        	int i = 0;
        	for(FieldMetadata f : pkfs) {
        		Column col = record.getColumn(i);
        		String item;
        		if(col.getType()==Type.INT || col.getType()==Type.LONG || col.getType()==Type.DOUBLE) {
        			item = MessageFormat.format(nodeWherePattern, "a", f.getName(), col.asLong().toString());
        		} else { // 其他，字符增加引号
        			item = MessageFormat.format(nodeWherePattern, "a", f.getName(), "'"+col.asString()+"'");
        		}
        		nodeWhereItems.add(item);	
        		i++;
        	}
        	// to节点
        	for(FKField fkf : fkfs) {
        		Column col = record.getColumn(i);
        		String item;
        		if(col.getType()==Type.INT || col.getType()==Type.LONG || col.getType()==Type.DOUBLE) {
        			item = MessageFormat.format(nodeWherePattern, "b", fkf.getRefField().getName(), col.asLong().toString());
        		} else { // 其他，字符增加引号
        			item = MessageFormat.format(nodeWherePattern, "b", fkf.getRefField().getName(), "'"+col.asString()+"'");
        		}
        		nodeWhereItems.add(item);	
        		i++;	

        	}
        	String nodeWhere = Utils.relWhere("", nodeWhereItems, " and ");
        	//
        	String cql = MessageFormat.format(Neo4jQueryPattern.CREATE_REL, 
        					from.getName(), to.getName(), nodeWhere.toString(), fkName, propsStr);
        	return new Query(cql);
        }
        
        // 连接表关系
        private Query calcLinkRelWriteQuery(TableMetadata from, TableMetadata to, TableMetadata link, 
        		FKConstraintMetadata fromFkmd, FKConstraintMetadata toFkmd,
        		Map<String, Object> props) {
        	// 连接属性
        	String propsStr = ParamsUtils.params2String(props);
        	// 节点过滤条件，使用主键
        	String nodeWherePattern = "{0}.{1} = {2}";
        	List<String> nodeWhereItems = new ArrayList<>();
        	// 
        	List<FKField> fromFkFs = fromFkmd.getFkFields();
        	List<FKField> toFkFs = toFkmd.getFkFields();
        	// from节点
        	for(FKField fkf : fromFkFs) {
        		String item;
        		// from<-link
        		// from表对应link外键名称
        		String fn = fkf.getRefField().getName();
        		String ln = fkf.getField().getName();
        		Object v = props.get(ln);
        		if(v instanceof Long || v instanceof Double) {
        			item = MessageFormat.format(nodeWherePattern, "a", fn, v.toString());
        		} else { // 其他，字符增加引号, 
        			item = MessageFormat.format(nodeWherePattern, "a", fn, "'"+v.toString()+"'");
        		}
        		nodeWhereItems.add(item);	
        		
        	}
        	// to节点
        	for(FKField fkf : toFkFs) {
        		String item;
        		// link->to
        		// to表对应link外键名称
        		String fn = fkf.getRefField().getName();
        		String ln = fkf.getField().getName();
        		Object v = props.get(ln);
        		if(v instanceof Long || v instanceof Double) {
        			item = MessageFormat.format(nodeWherePattern, "b", fn, v.toString());
        		} else { // 其他，字符增加引号, 
        			item = MessageFormat.format(nodeWherePattern, "b", fn, "'"+v.toString()+"'");
        		}
        		nodeWhereItems.add(item);	
        	}
        	String nodeWhere = Utils.relWhere("", nodeWhereItems, " and ");
        	// from->to
        	String cql = MessageFormat.format(Neo4jQueryPattern.CREATE_REL, 
        					from.getName(), to.getName(), nodeWhere.toString(), link.getName(), propsStr);
        	return new Query(cql);
        }
          
        // 节点或关系属性，类型转换
        // 本地缓存，查找
		protected Map<String, Object> fillProperties(TableMetadata tbmd, Record record) {
			Map<String, Object> props = new HashMap<>();
			int i = 0;
			for(FieldMetadata fmd : tbmd.getFields()) {
				String columnName = fmd.getName();
				JDBCType jdbcType = fmd.getType();
				Column column = record.getColumn(i);
				/* BAD, NULL, INT, LONG, DOUBLE, STRING, BOOL, DATE, BYTES */
				switch (column.getType()) {
					case INT:
					case LONG:
						props.put(columnName, column.asLong());
						break;
					case DOUBLE:
						props.put(columnName, column.asDouble());
						break;
					case STRING:
						// 转义
						String col = Utils.strFieldEscape(column.asString());
						props.put(columnName, col);
						break;
					case BOOL:
						props.put(fmd.getName(), column.asBoolean());
						break;
					case DATE:
						Date date = column.asDate();
						// LocalDateTime ldt = Utils.dateToLocalDateTime(date);
						props.put(fmd.getName(), date);
						break;				
					case BYTES:
						log.warn(String.format("neo4j不支持二进制属性类型, 字段名:[%s], 字段类型:[%s]. ",
								fmd.getName(),
								jdbcType.getName()));
						break;
					default: // 其他，不支持类型
						throw DataXException.asDataXException(Neo4jWriterErrorCode.UNSUPPORTED_TYPE, 
								String.format("neo4j不支持属性类型, 字段名:[%s], 字段类型:[%s]. ",
										fmd.getName(),
										jdbcType.getName()));
				} // end switch
				i++; // 下一个字段
			} // end for tbmd
			return props;
		}
        
        // 后处理
        @Override
        public void post() {
        }

        // 释放
        @Override
        public void destroy() {

        }
    }
}
