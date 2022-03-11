package com.alibaba.datax.plugin.reader.oceanbasev10reader.ext;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.statistics.PerfRecord;
import com.alibaba.datax.common.statistics.PerfTrace;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.CommonRdbmsReader;
import com.alibaba.datax.plugin.rdbms.reader.Constant;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.RdbmsException;
import com.alibaba.datax.plugin.reader.oceanbasev10reader.Config;
import com.alibaba.datax.plugin.reader.oceanbasev10reader.util.ObReaderUtils;
import com.alibaba.datax.plugin.reader.oceanbasev10reader.util.TaskContext;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class ReaderTask extends CommonRdbmsReader.Task {
    private static final Logger LOG = LoggerFactory.getLogger(ReaderTask.class);
    private int taskGroupId = -1;
    private int taskId = -1;

    private String username;
    private String password;
    private String jdbcUrl;
    private String mandatoryEncoding;
    private int queryTimeoutSeconds;// 查询超时 默认48小时
    private int readBatchSize;
    private int retryLimit = 0;
    private String compatibleMode = ObReaderUtils.OB_COMPATIBLE_MODE_MYSQL;
    private boolean reuseConn = false;

    public ReaderTask(int taskGroupId, int taskId) {
        super(ObReaderUtils.databaseType, taskGroupId, taskId);
        this.taskGroupId = taskGroupId;
        this.taskId = taskId;
    }

    @Override
    public void init(Configuration readerSliceConfig) {
        /* for database connection */
        username = readerSliceConfig.getString(Key.USERNAME);
        password = readerSliceConfig.getString(Key.PASSWORD);
        jdbcUrl = readerSliceConfig.getString(Key.JDBC_URL);
        queryTimeoutSeconds = readerSliceConfig.getInt(Config.QUERY_TIMEOUT_SECOND,
                Config.DEFAULT_QUERY_TIMEOUT_SECOND);
        // ob10的处理
        if (jdbcUrl.startsWith(com.alibaba.datax.plugin.rdbms.writer.Constant.OB10_SPLIT_STRING)) {
            String[] ss = jdbcUrl.split(com.alibaba.datax.plugin.rdbms.writer.Constant.OB10_SPLIT_STRING_PATTERN);
            if (ss.length == 3) {
                LOG.info("this is ob1_0 jdbc url.");
                username = ss[1].trim() + ":" + username;
                jdbcUrl = ss[2];
            }
        }

        jdbcUrl = jdbcUrl.replace("jdbc:mysql:", "jdbc:oceanbase:") + "&socketTimeout=1800000&connectTimeout=60000"; //socketTimeout 半个小时
        if (ObReaderUtils.compatibleMode.equals(ObReaderUtils.OB_COMPATIBLE_MODE_ORACLE)) {
            compatibleMode = ObReaderUtils.OB_COMPATIBLE_MODE_ORACLE;
        }
        LOG.info("this is ob1_0 jdbc url. user=" + username + " :url=" + jdbcUrl);
        mandatoryEncoding = readerSliceConfig.getString(Key.MANDATORY_ENCODING, "");
        retryLimit = readerSliceConfig.getInt(Config.RETRY_LIMIT, Config.DEFAULT_RETRY_LIMIT);
        LOG.info("retryLimit: " + retryLimit);
    }

    private void buildSavePoint(TaskContext context) {
        if (!ObReaderUtils.isUserSavePointValid(context)) {
            LOG.info("user save point is not valid, set to null.");
            context.setUserSavePoint(null);
        }
    }

    /**
     * 如果isTableMode && table有PK
     * <p>
     * 则支持断点续读 (若pk不在原始的columns中,则追加到尾部,但不传给下游)
     * <p>
     * 否则,则使用旧模式
     */
    @Override
    public void startRead(Configuration readerSliceConfig, RecordSender recordSender,
                          TaskPluginCollector taskPluginCollector, int fetchSize) {
        String querySql = readerSliceConfig.getString(Key.QUERY_SQL);
        String table = readerSliceConfig.getString(Key.TABLE);
        PerfTrace.getInstance().addTaskDetails(taskId, table + "," + jdbcUrl);
        List<String> columns = readerSliceConfig.getList(Key.COLUMN_LIST, String.class);
        String where = readerSliceConfig.getString(Key.WHERE);
        boolean weakRead = readerSliceConfig.getBool(Key.WEAK_READ, true); // default true, using weak read
        String userSavePoint = readerSliceConfig.getString(Key.SAVE_POINT, null);
        reuseConn = readerSliceConfig.getBool(Key.REUSE_CONN, false);
        String partitionName = readerSliceConfig.getString(Key.PARTITION_NAME, null);
        // 从配置文件中取readBatchSize,若无则用默认值
        readBatchSize = readerSliceConfig.getInt(Config.READ_BATCH_SIZE, Config.DEFAULT_READ_BATCH_SIZE);
        // 不能少于1万
        if (readBatchSize < 10000) {
            readBatchSize = 10000;
        }
        TaskContext context = new TaskContext(table, columns, where, fetchSize);
        context.setQuerySql(querySql);
        context.setWeakRead(weakRead);
        context.setCompatibleMode(compatibleMode);
        if (partitionName != null) {
            context.setPartitionName(partitionName);
        }
        // Add the user save point into the context
        context.setUserSavePoint(userSavePoint);
        PerfRecord allPerf = new PerfRecord(taskGroupId, taskId, PerfRecord.PHASE.RESULT_NEXT_ALL);
        allPerf.start();
        boolean isTableMode = readerSliceConfig.getBool(Constant.IS_TABLE_MODE);
        try {
            startRead0(isTableMode, context, recordSender, taskPluginCollector);
        } finally {
            ObReaderUtils.close(null, null, context.getConn());
        }
        allPerf.end(context.getCost());
        // 目前大盘是依赖这个打印，而之前这个Finish read record是包含了sql查询和result next的全部时间
        LOG.info("finished read record by Sql: [{}\n] {}.", context.getQuerySql(), jdbcUrl);
    }

    private void startRead0(boolean isTableMode, TaskContext context, RecordSender recordSender,
                            TaskPluginCollector taskPluginCollector) {
        // 不是table模式 直接使用原来的做法
        if (!isTableMode) {
            doRead(recordSender, taskPluginCollector, context);
            return;
        }
        // check primary key index
        Connection conn = DBUtil.getConnection(ObReaderUtils.databaseType, jdbcUrl, username, password);
        ObReaderUtils.initConn4Reader(conn, queryTimeoutSeconds);
        context.setConn(conn);
        try {
            ObReaderUtils.initIndex(conn, context);
            ObReaderUtils.matchPkIndexs(conn, context);
        } catch (Throwable e) {
            LOG.warn("fetch PkIndexs fail,table=" + context.getTable(), e);
        }
        // 如果不是table 且 pk不存在 则仍然使用原来的做法
        if (context.getPkIndexs() == null) {
            doRead(recordSender, taskPluginCollector, context);
            return;
        }

        // setup the user defined save point
        buildSavePoint(context);

        // 从这里开始就是 断点续读功能
        // while(true) {
        // 正常读 (需 order by pk asc)
        // 如果遇到失败,分两种情况:
        // a)已读出记录,则开始走增量读逻辑
        // b)未读出记录,则走正常读逻辑(仍然需要order by pk asc)
        // 正常结束 则 break
        // }
        context.setReadBatchSize(readBatchSize);
        String getFirstQuerySql = ObReaderUtils.buildFirstQuerySql(context);
        String appendQuerySql = ObReaderUtils.buildAppendQuerySql(conn, context);
        LOG.warn("start table scan key : {}", context.getIndexName() == null ? "primary" : context.getIndexName());
        context.setQuerySql(getFirstQuerySql);
        boolean firstQuery = true;
        // 原来打算firstQuery时 limit 1 减少
        // 后来经过对比发现其实是多余的,因为:
        // 1.假如走gmt_modified辅助索引,则直接索引扫描 不需要topN的order by
        // 2.假如不走辅助索引,而是pk table scan,则减少排序规模并没有好处,因为下一次仍然要排序
        // 减少这个多余的优化tip 可以让代码更易读
        int retryCount = 0;
        while (true) {
            try {
                boolean finish = doRead(recordSender, taskPluginCollector, context);
                if (finish) {
                    break;
                }
            } catch (Throwable e) {
                if (retryLimit == ++retryCount) {
                    throw RdbmsException.asQueryException(ObReaderUtils.databaseType, new Exception(e),
                            context.getQuerySql(), context.getTable(), username);
                }
                LOG.error("read fail, retry count " + retryCount + ", sleep 60 second, save point:" +
                        context.getSavePoint() + ", error: " + e.getMessage());
                ObReaderUtils.sleep(60000); // sleep 10s
            }
            // 假如原来的查询有查出数据,则改成增量查询
            if (firstQuery && context.getPkIndexs() != null && context.getSavePoint() != null) {
                context.setQuerySql(appendQuerySql);
                firstQuery = false;
            }
        }
        DBUtil.closeDBResources(null, context.getConn());
    }

    private boolean isConnectionAlive(Connection conn) {
        if (conn == null) {
            return false;
        }
        Statement stmt = null;
        ResultSet rs = null;
        String sql = "select 1" + (compatibleMode == ObReaderUtils.OB_COMPATIBLE_MODE_ORACLE ? " from dual" : "");
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
            rs.next();
        } catch (Exception ex) {
            LOG.info("connection is not alive: " + ex.getMessage());
            return false;
        } finally {
            DBUtil.closeDBResources(rs, stmt, null);
        }
        return true;
    }

    private boolean doRead(RecordSender recordSender, TaskPluginCollector taskPluginCollector, TaskContext context) {
        LOG.info("exe sql: {}", context.getQuerySql());
        Connection conn = context.getConn();
        if (reuseConn && isConnectionAlive(conn)) {
            LOG.info("connection is alive, will reuse this connection.");
        } else {
            LOG.info("Create new connection for reader.");
            conn = DBUtil.getConnection(ObReaderUtils.databaseType, jdbcUrl, username, password);
            ObReaderUtils.initConn4Reader(conn, queryTimeoutSeconds);
            context.setConn(conn);
        }
        PreparedStatement ps = null;
        ResultSet rs = null;
        PerfRecord perfRecord = new PerfRecord(taskGroupId, taskId, PerfRecord.PHASE.SQL_QUERY);
        perfRecord.start();
        try {
            ps = conn.prepareStatement(context.getQuerySql(),
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            if (context.getPkIndexs() != null && context.getSavePoint() != null) {
                Record savePoint = context.getSavePoint();
                List<Column> point = ObReaderUtils.buildPoint(savePoint, context.getPkIndexs());
                ObReaderUtils.binding(ps, point);
                if (LOG.isWarnEnabled()) {
                    List<String> pointForLog = new ArrayList<String>();
                    for (Column c : point) {
                        pointForLog.add(c.asString());
                    }
                    LOG.warn("{} save point : {}", context.getTable(), StringUtils.join(pointForLog, ','));
                }
            }
            // 打开流式接口
            ps.setFetchSize(context.getFetchSize());
            rs = ps.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            int columnNumber = metaData.getColumnCount();
            long lastTime = System.nanoTime();
            int count = 0;
            for (; rs.next(); count++) {
                context.addCost(System.nanoTime() - lastTime);
                Record row = buildRecord(recordSender, rs, metaData, columnNumber, mandatoryEncoding,
                        taskPluginCollector);
                // // 如果第一个record重复了,则不需要发送
                // if (count == 0 &&
                // ObReaderUtils.isPkEquals(context.getSavePoint(), row,
                // context.getPkIndexs())) {
                // continue;
                // }
                // 如果是querySql
                if (context.getTransferColumnNumber() == -1
                        || row.getColumnNumber() == context.getTransferColumnNumber()) {
                    recordSender.sendToWriter(row);
                } else {
                    Record newRow = recordSender.createRecord();
                    for (int i = 0; i < context.getTransferColumnNumber(); i++) {
                        newRow.addColumn(row.getColumn(i));
                    }
                    recordSender.sendToWriter(newRow);
                }
                context.setSavePoint(row);
                lastTime = System.nanoTime();
            }
            LOG.info("end of sql: {}, " + count + "rows are read.", context.getQuerySql());
            return context.getReadBatchSize() <= 0 || count < readBatchSize;
        } catch (Exception e) {
            ObReaderUtils.close(null, null, context.getConn());
            context.setConn(null);
            LOG.error("reader data fail", e);
            throw RdbmsException.asQueryException(ObReaderUtils.databaseType, e, context.getQuerySql(),
                    context.getTable(), username);
        } finally {
            perfRecord.end();
            if (reuseConn) {
                ObReaderUtils.close(rs, ps, null);
            } else {
                ObReaderUtils.close(rs, ps, conn);
            }
        }
    }
}
