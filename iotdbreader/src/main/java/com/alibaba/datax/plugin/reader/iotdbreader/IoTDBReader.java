package com.alibaba.datax.plugin.reader.iotdbreader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.AbstractTaskPlugin;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.common.block.column.NullColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class IoTDBReader extends Reader {
    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private Configuration jobConf;

        /**
         * Job对象初始化工作
         */
        @Override
        public void init() {
            // 通过super.getPluginJobConf()获取与本插件相关的配置。
            this.jobConf = super.getPluginJobConf();
            // 检查各种参数是否正确
            String username = this.jobConf.getString(Key.USERNAME);
            if (username == null || username.isEmpty()) {
                throw DataXException.asDataXException(IoTDBReaderErrorCode.REQUIRED_VALUE, "The parameter [" + Key.USERNAME + "] is not set.");
            }
            String password = this.jobConf.getString(Key.PASSWORD);
            if (password == null || password.isEmpty()) {
                throw DataXException.asDataXException(IoTDBReaderErrorCode.REQUIRED_VALUE, "The parameter [" + Key.PASSWORD + "] is not set.");
            }
            String host = this.jobConf.getString(Key.HOST);
            if (host == null || host.isEmpty()) {
                throw DataXException.asDataXException(IoTDBReaderErrorCode.REQUIRED_VALUE, "The parameter [" + Key.HOST + "] is not set.");
            }
            String port = this.jobConf.getString(Key.PORT);
            if (port == null || port.isEmpty()) {
                throw DataXException.asDataXException(IoTDBReaderErrorCode.REQUIRED_VALUE, "The parameter [" + Key.PORT + "] is not set.");
            }
            String fetchSize = this.jobConf.getString(Key.FETCH_SIZE);
            if (fetchSize == null || fetchSize.isEmpty()) {
                throw DataXException.asDataXException(IoTDBReaderErrorCode.REQUIRED_VALUE, "The parameter [" + Key.FETCH_SIZE + "] is not set.");
            }
            // 还有一部分参数没检查，没必要了。

        }
        @Override
        public void prepare() {
        }

        /**
         * 将Job拆分Task。
         * @param adviceNumber 框架建议的拆分数，一般是运行时所配置的并发度。
         * @return Task的配置列表，一个配置文件对应一个task。
         */
        @Override
        public List<Configuration> split(int adviceNumber) {
            // 每个config对应一个task
            List<Configuration> configs = new ArrayList<>();
            List<String> queryList = this.jobConf.getList(Key.QUERY_SQLS, String.class);
            if (queryList == null || queryList.size() == 0){
                Configuration clone = this.jobConf.clone();
                String device = this.jobConf.getString(Key.DEVICE);
                List<String> measurements = this.jobConf.getList(Key.MEASUREMENTS, String.class);
                String where = this.jobConf.getString(Key.WHERE);
                StringBuilder sb = new StringBuilder();
                sb.append("select ").append(String.join(",", measurements));
                sb.append(" from ").append(device);
                if (where != null && !where.isEmpty()){
                    sb.append(" where ").append(where);
                }
                clone.set(Key.QUERY_SQL, sb.toString());
                configs.add(clone);
                //DataX中一个查询是单线程，实际上底层session中是多线程读取。
            }else{
                // 直接读取最终SQL
                for (String query : queryList) {
                    Configuration clone = this.jobConf.clone();
                    clone.remove(Key.QUERY_SQLS);
                    clone.set(Key.QUERY_SQL, query);
                    configs.add(clone);
                }
            }
            // LOG.info("configs: {}", configs);
            return configs;
        }

        /**
         * Job对象自身的销毁工作。
         */
        @Override
        public void destroy() {

        }

        /**
         * 全局的后置工作。
         */
        @Override
        public void post() {

        }


    }

    public static class Task extends Reader.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration taskConf;

        /**
         * IoTDB原生读写工具
         */
        private Session session;
        /**
         * IoTDB中的时间列插入的位置，默认为0，即第一列。
         */
        private int timeColumnPosition;
        /**
         * 最终的查询SQL，交给session执行。
         */
        private String querySql;
        private TaskPluginCollector taskPluginCollector;

        @Override
        public void init() {
            // 获取与本Task相关的配置，是Job的split方法返回的配置列表中的其中一个。
            taskConf = super.getPluginJobConf();

            // session init
            session =
                    new Session.Builder()
                            .host(taskConf.getString(Key.HOST))
                            .port(taskConf.getInt(Key.PORT))
                            .username(taskConf.getString(Key.USERNAME))
                            .password(taskConf.getString(Key.PASSWORD))
                            .version(Version.valueOf(taskConf.getString(Key.VERSION)))
                            .build();
            // open session, close RPCCompression
            try {
                session.open(false);
            } catch (IoTDBConnectionException e) {
                throw new RuntimeException(e);
            }

            // set session fetchSize
            session.setFetchSize(taskConf.getInt(Key.FETCH_SIZE));

            this.timeColumnPosition = (taskConf.getInt(Key.TIME_COLUMN_POSITION) == null) ? 0 : taskConf.getInt(Key.TIME_COLUMN_POSITION);
            this.querySql = taskConf.getString(Key.QUERY_SQL);
            taskPluginCollector = super.getTaskPluginCollector();
        }

        @Override
        public void destroy() {
            // Task自身的销毁工作。
            try {
                if (session != null){
                    session.close();
                }
            } catch (IoTDBConnectionException e) {
                LOG.info(e.getMessage());
            }
        }

        /**
         * 从数据源读数据，写入到RecordSender中。
         * @param recordSender 把数据写入连接Reader和Writer的缓存队列。
         */
        @Override
        public void startRead(RecordSender recordSender) {
            try {
                SessionDataSet dataSet = session.executeQueryStatement(this.querySql);
                while (dataSet.hasNext()) {
                    // DataX中的行record
                    Record record = recordSender.createRecord();
                    // IoTDB中的行
                    RowRecord rowRecord = dataSet.next();
                    List<Field> fields = rowRecord.getFields();
                    try {
                        // 除time列外的其他列遍历类型后转换
                        for (int i = 0; i < fields.size(); i++) {
                            if (i == timeColumnPosition){
                                // time列插入指定位置，时间列不在fields中，需要单独处理。不能为null
                                long timestamp = rowRecord.getTimestamp();
                                record.addColumn(new DateColumn(timestamp));
                            }
                            Field field = fields.get(i);
                            TSDataType dataType = field.getDataType();
                            if (dataType == null) {
                                // 需要写插件支持处理null数据，否则会空指向异常，这里先当成脏数据
                                // record.addColumn(null);
                                // continue;
                                throw new RuntimeException("null datatype");
                            }
                            switch (dataType) {
                                case BOOLEAN:
                                    record.addColumn(new BoolColumn(field.getBoolV()));
                                    break;
                                case INT32:
                                    record.addColumn(new LongColumn(field.getIntV()));
                                    break;
                                case INT64:
                                case TIMESTAMP:
                                    record.addColumn(new LongColumn(field.getLongV()));
                                    break;
                                case FLOAT:
                                    record.addColumn(new DoubleColumn(field.getFloatV()));
                                    break;
                                case DOUBLE:
                                    record.addColumn(new DoubleColumn(field.getDoubleV()));
                                    break;
                                case STRING:
                                case TEXT:
                                    record.addColumn(new StringColumn(field.getStringValue()));
                                    break;
                                case DATE:
                                    record.addColumn(new DateColumn(Date.valueOf(field.getDateV())));
                                    break;
                                default:
                                    throw new RuntimeException("Unsupported data type: " + dataType);
                            }
                        }
                        // 发送
                        recordSender.sendToWriter(record);
                    }catch (RuntimeException e){
                        LOG.info(e.getMessage());
                        this.taskPluginCollector.collectDirtyRecord(record, e);
                    }
                }
            } catch (StatementExecutionException | IoTDBConnectionException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
