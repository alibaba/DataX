package com.alibaba.datax.plugin.writer.iotdbwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.tsfile.enums.TSDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class IoTDBWriter extends Writer {

    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private Configuration jobConf;

        @Override
        public void init() {
            this.jobConf = super.getPluginJobConf();
            // 检查各种参数是否正确
            String username = this.jobConf.getString(Key.USERNAME);
            if (username == null || username.isEmpty()) {
                throw DataXException.asDataXException(IoTDBWriterErrorCode.REQUIRED_VALUE, "The parameter [" + Key.USERNAME + "] is not set.");
            }
            String password = this.jobConf.getString(Key.PASSWORD);
            if (password == null || password.isEmpty()) {
                throw DataXException.asDataXException(IoTDBWriterErrorCode.REQUIRED_VALUE, "The parameter [" + Key.PASSWORD + "] is not set.");
            }
            String host = this.jobConf.getString(Key.HOST);
            if (host == null || host.isEmpty()) {
                throw DataXException.asDataXException(IoTDBWriterErrorCode.REQUIRED_VALUE, "The parameter [" + Key.HOST + "] is not set.");
            }
            String port = this.jobConf.getString(Key.PORT);
            if (port == null || port.isEmpty()) {
                throw DataXException.asDataXException(IoTDBWriterErrorCode.REQUIRED_VALUE, "The parameter [" + Key.PORT + "] is not set.");
            }
            // 还有一部分参数没检查，没必要了。
        }

        @Override
        public void prepare(){
            // 写入前准备，IOTDB不需要提前创建表。
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> configs = new ArrayList<>();
            // 根据源端划分多个task，每个写task对应一个读task，并行插入下放到session批次处理。
            for (int i = 0; i < mandatoryNumber; i++) {
                Configuration clone = this.jobConf.clone();
                configs.add(clone);
            }
            // LOG.info("configs: {}", configs);
            return configs;
        }

        @Override
        public void destroy() {
        }

    }

    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);
        private Configuration taskConf;

        // IoTDB原生读写工具
        private Session session;

        // 是否在插入前删除已有的时间序列，为""表示不执行
        // private String deleteBeforeInsert;

        // 插入批次大小
        private int insertBatchSize;

        // IoTDB中的时间列插入的位置，默认为0，即第一列。
        private int timeColumnPosition;

        // 处理脏数据
        private TaskPluginCollector taskPluginCollector;

        // 预先执行的SQL语句
        private List<String> preSqls;


        @Override
        public void init() {
            // 获取与本Task相关的配置，是Job的split方法返回的配置列表中的其中一个。
            this.taskConf = super.getPluginJobConf();

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

            // 获取参数，否则默认值
            insertBatchSize = (taskConf.getInt(Key.BATCH_SIZE) == null) ? 1000 : taskConf.getInt(Key.BATCH_SIZE);
            timeColumnPosition = (taskConf.getInt(Key.TIME_COLUMN_POSITION) == null) ? 0 : taskConf.getInt(Key.TIME_COLUMN_POSITION);
            preSqls = (taskConf.getList(Key.PRE_SQL, String.class) == null) ? new ArrayList<>() : taskConf.getList(Key.PRE_SQL, String.class);
            taskPluginCollector = super.getTaskPluginCollector();
        }

        @Override
        public void prepare() {
            if (preSqls.size() != 0){
                for (String sql : preSqls) {
                    try {
                        session.executeNonQueryStatement(sql);

                    } catch (IoTDBConnectionException | StatementExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }
                LOG.info("=======Complated preSqls=======");
            }

            // IoTDB会自动创建时间序列，无需提前创建
        }

        @Override
        public void destroy() {
            try {
                if (session != null){
                    session.close();
                }
            } catch (IoTDBConnectionException e) {
                LOG.info(e.getMessage());
            }
        }

        /**
         * 从RecordReceiver中读取数据，写入目标数据源。
         * @param lineReceiver 数据来自Reader和Writer之间的缓存队列。
         */
        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            // 往一个设备device中插入数据
            Record record = null;
            try{
                // 构建List，一个设备批量写入
                String device = taskConf.getString(Key.DEVICE);
                List<Long> timestamps = new ArrayList<>();
                List<List<String>> measurementsList = new ArrayList<>();
                List<List<Object>> valuesList = new ArrayList<>();
                List<List<TSDataType>> typesList = new ArrayList<>();

                // 获取Record记录，传输结束返回null
                int count;  // 统计插入记录数
                for (count = 0; (record = lineReceiver.getFromReader()) != null; count++) {
                    // 处理时间列
                    timestamps.add(record.getColumn(timeColumnPosition).asLong());
                    // 处理测点
                    List<String> measurements = taskConf.getList(Key.MEASUREMENTS, String.class);
                    measurementsList.add(measurements);
                    // 处理类型和值
                    List<TSDataType> types = new ArrayList<>();
                    List<Object> values = new ArrayList<>();
                    try{
                        for (int i = 0; i < record.getColumnNumber(); i++) {
                            if (i == timeColumnPosition){
                                continue;  // 跳过时间列
                            }
                            Column col = record.getColumn(i);
                            switch (col.getType()) {
                                case BOOL:
                                    types.add(TSDataType.BOOLEAN);
                                    values.add(col.asBoolean());
                                    break;
                                case INT:
                                    types.add(TSDataType.INT32);
                                    values.add((Integer) col.getRawData());
                                    break;
                                case LONG:
                                    types.add(TSDataType.INT64);
                                    values.add(col.asLong());
                                    break;
                                case DOUBLE:
                                    types.add(TSDataType.DOUBLE);
                                    values.add(col.asDouble());
                                    break;
                                case NULL:
                                    // IoTDB可以处理null
                                    types.add(null);
                                    values.add(null);
                                    break;
                                case STRING:
                                    types.add(TSDataType.STRING);
                                    values.add(col.asString());
                                    break;
                                case DATE:
                                    types.add(TSDataType.DATE);
                                    values.add(col.asDate());
                                    break;
                                case BAD:
                                default:
                                    throw new RuntimeException("unsupported type:" + col.getType());
                            }
                        }
                        typesList.add(types);
                        valuesList.add(values);
                    }catch (RuntimeException e){
                        LOG.info(e.getMessage());
                        taskPluginCollector.collectDirtyRecord(record, e);
                    }

                    if (count != 0 && count % insertBatchSize == 0) {
                        session.insertRecordsOfOneDevice(device, timestamps, measurementsList, typesList, valuesList);
                        timestamps.clear();
                        measurementsList.clear();
                        typesList.clear();
                        valuesList.clear();
                    }
                }
                if (!timestamps.isEmpty()){
                    session.insertRecordsOfOneDevice(device, timestamps, measurementsList, typesList, valuesList);
                }
                LOG.info("========= task all data inserted, total record: " + (count-1));
            }catch (IoTDBConnectionException | StatementExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
