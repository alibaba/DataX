package com.alibaba.datax.plugin.writer.cassandrawriter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.datastax.driver.core.querybuilder.QueryBuilder.timestamp;

/**
 * Created by mazhenlin on 2019/8/19.
 */
public class CassandraWriter extends Writer {
  private static final Logger LOG = LoggerFactory
      .getLogger(CassandraWriter.class);
  public static class Job extends Writer.Job {
    private Configuration originalConfig = null;

    @Override public List<Configuration> split(int mandatoryNumber) {
      List<Configuration> splitResultConfigs = new ArrayList<Configuration>();
      for (int j = 0; j < mandatoryNumber; j++) {
        splitResultConfigs.add(originalConfig.clone());
      }
      return splitResultConfigs;

    }

    @Override public void init() {
      originalConfig = getPluginJobConf();
    }

    @Override public void destroy() {

    }
  }

  public static class Task extends Writer.Task {
    private Configuration taskConfig;
    private Cluster cluster = null;
    private Session session = null;
    private PreparedStatement statement = null;
    private int columnNumber = 0;
    private List<DataType> columnTypes;
    private List<String> columnMeta = null;
    private int writeTimeCol = -1;
    private boolean asyncWrite = false;
    private long batchSize = 1;
    private List<ResultSetFuture> unConfirmedWrite;
    private List<BoundStatement> bufferedWrite;

    @Override public void startWrite(RecordReceiver lineReceiver) {
      try {
        Record record;
        while ((record = lineReceiver.getFromReader()) != null) {
          if (record.getColumnNumber() != columnNumber) {
            // 源头读取字段列数与目的表字段写入列数不相等，直接报错
            throw DataXException
                .asDataXException(
                    CassandraWriterErrorCode.CONF_ERROR,
                    String.format(
                        "列配置信息有错误. 因为您配置的任务中，源头读取字段数:%s 与 目的表要写入的字段数:%s 不相等. 请检查您的配置并作出修改.",
                        record.getColumnNumber(),
                        this.columnNumber));
          }

          BoundStatement boundStmt = statement.bind();
          for (int i = 0; i < columnNumber; i++) {
            if( writeTimeCol != -1 && i == writeTimeCol ) {
              continue;
            }
            Column col = record.getColumn(i);
            int pos = i;
            if( writeTimeCol != -1 && pos > writeTimeCol ) {
              pos = i - 1;
            }
            CassandraWriterHelper.setupColumn(boundStmt,pos,columnTypes.get(pos),col);
          }
          if(writeTimeCol != -1) {
            Column col = record.getColumn(writeTimeCol );
            boundStmt.setLong(columnNumber - 1,col.asLong());
          }
          if( batchSize <= 1 ) {
            session.execute(boundStmt);
          } else {
            if( asyncWrite ) {
              unConfirmedWrite.add(session.executeAsync(boundStmt));
              if (unConfirmedWrite.size() >= batchSize) {
                for (ResultSetFuture write : unConfirmedWrite) {
                  write.getUninterruptibly(10000, TimeUnit.MILLISECONDS);
                }
                unConfirmedWrite.clear();
              }
            } else {
              bufferedWrite.add(boundStmt);
              if( bufferedWrite.size() >= batchSize ) {
                BatchStatement batchStatement = new BatchStatement(Type.UNLOGGED);
                batchStatement.addAll(bufferedWrite);
                try {
                  session.execute(batchStatement);
                } catch (Exception e ) {
                  LOG.error("batch写入失败，尝试逐条写入.",e);
                  for( BoundStatement stmt: bufferedWrite ) {
                    session.execute(stmt);
                  }
                }
                ///LOG.info("batch finished. size = " + bufferedWrite.size());
                bufferedWrite.clear();
              }
            }
          }

        }
        if( unConfirmedWrite != null && unConfirmedWrite.size() > 0 ) {
          for( ResultSetFuture write : unConfirmedWrite ) {
            write.getUninterruptibly(10000, TimeUnit.MILLISECONDS);
          }
          unConfirmedWrite.clear();
        }
        if( bufferedWrite !=null && bufferedWrite.size() > 0 ) {
          BatchStatement batchStatement = new BatchStatement(Type.UNLOGGED);
          batchStatement.addAll(bufferedWrite);
          session.execute(batchStatement);
          bufferedWrite.clear();
        }
      } catch (Exception e) {
        throw DataXException.asDataXException(
            CassandraWriterErrorCode.WRITE_DATA_ERROR, e);
      }
    }


    @Override public void init() {
      this.taskConfig = super.getPluginJobConf();
      String username = taskConfig.getString(Key.USERNAME);
      String password = taskConfig.getString(Key.PASSWORD);
      String hosts = taskConfig.getString(Key.HOST);
      Integer port = taskConfig.getInt(Key.PORT,9042);
      boolean useSSL = taskConfig.getBool(Key.USESSL);
      String keyspace = taskConfig.getString(Key.KEYSPACE);
      String table = taskConfig.getString(Key.TABLE);
      batchSize = taskConfig.getLong(Key.BATCH_SIZE,1);
      this.columnMeta = taskConfig.getList(Key.COLUMN,String.class);
      columnTypes = new ArrayList<DataType>(columnMeta.size());
      columnNumber = columnMeta.size();
      asyncWrite = taskConfig.getBool(Key.ASYNC_WRITE,false);

      int connectionsPerHost = taskConfig.getInt(Key.CONNECTIONS_PER_HOST,8);
      int maxPendingPerConnection = taskConfig.getInt(Key.MAX_PENDING_CONNECTION,128);
      PoolingOptions poolingOpts = new PoolingOptions()
          .setConnectionsPerHost(HostDistance.LOCAL, connectionsPerHost, connectionsPerHost)
          .setMaxRequestsPerConnection(HostDistance.LOCAL, maxPendingPerConnection)
          .setNewConnectionThreshold(HostDistance.LOCAL, 100);
      Cluster.Builder clusterBuilder = Cluster.builder().withPoolingOptions(poolingOpts);
      if ((username != null) && !username.isEmpty()) {
        clusterBuilder = clusterBuilder.withCredentials(username, password)
            .withPort(Integer.valueOf(port)).addContactPoints(hosts.split(","));
        if (useSSL) {
          clusterBuilder = clusterBuilder.withSSL();
        }
      } else {
        clusterBuilder = clusterBuilder.withPort(Integer.valueOf(port))
            .addContactPoints(hosts.split(","));
      }
      cluster = clusterBuilder.build();
      session = cluster.connect(keyspace);
      TableMetadata meta = cluster.getMetadata().getKeyspace(keyspace).getTable(table);

      Insert insertStmt = QueryBuilder.insertInto(table);
      for( String colunmnName : columnMeta ) {
        if( colunmnName.toLowerCase().equals(Key.WRITE_TIME) ) {
          if( writeTimeCol != -1 ) {
            throw DataXException
                .asDataXException(
                    CassandraWriterErrorCode.CONF_ERROR,
                    "列配置信息有错误. 只能有一个时间戳列(writetime())");
          }
          writeTimeCol = columnTypes.size();
          continue;
        }
        insertStmt.value(colunmnName,QueryBuilder.bindMarker());
        ColumnMetadata col = meta.getColumn(colunmnName);
        if( col == null ) {
          throw DataXException
              .asDataXException(
                  CassandraWriterErrorCode.CONF_ERROR,
                  String.format(
                      "列配置信息有错误. 表中未找到列名 '%s' .",
                      colunmnName));
        }
        columnTypes.add(col.getType());
      }
      if(writeTimeCol != -1) {
        insertStmt.using(timestamp(QueryBuilder.bindMarker()));
      }
      String cl = taskConfig.getString(Key.CONSITANCY_LEVEL);
      if( cl != null && !cl.isEmpty() ) {
        insertStmt.setConsistencyLevel(ConsistencyLevel.valueOf(cl));
      } else {
        insertStmt.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
      }

      statement = session.prepare(insertStmt);

      if( batchSize > 1 ) {
        if( asyncWrite ) {
          unConfirmedWrite = new ArrayList<ResultSetFuture>();
        } else {
          bufferedWrite = new ArrayList<BoundStatement>();
        }
      }

    }

    @Override public void destroy() {

    }
  }
}
