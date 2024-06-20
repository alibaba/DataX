package com.alibaba.datax.plugin.writer.iotdbwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
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

import java.util.ArrayList;
import java.util.List;

public class IoTDBWriter extends Writer {

    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private Configuration originalConfig;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            // TODO 检查配置文件参数

        }

        @Override
        public void destroy() {

        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            // TODO 根据什么拆分Task？
            List<Configuration> configs = new ArrayList<>();
            for (int i = 0; i < mandatoryNumber; i++) {
                configs.add(originalConfig);
            }
            return configs;
        }
    }

    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);
        private Configuration writerSliceConfig;
        private TaskPluginCollector taskPluginCollector;

        private Session session;
        @Override
        public void init() {
            this.writerSliceConfig = getPluginJobConf();
            this.taskPluginCollector = super.getTaskPluginCollector();

            // session init
            session =
                    new Session.Builder()
                            .host("192.168.150.100")
                            .port(6667)
                            .username("root")
                            .password("root")
                            .version(Version.V_0_13)
                            .build();
            // open session, close RPCCompression
            try {
                session.open(false);
            } catch (IoTDBConnectionException e) {
                throw new RuntimeException(e);
            }

            // set session fetchSize
            session.setFetchSize(10000);

            // // 先删除已有的时间序列
            // if (session.checkTimeseriesExists(device + ".**")) {
            //     session.deleteTimeseries(device + ".**");
            //     System.out.println("删除已有的时间序列完成==============");
            // }
        }

        @Override
        public void destroy() {
            try {
                if (session != null){
                    session.close();
                }
            } catch (IoTDBConnectionException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            // 暂时实现往一个设备中插入数据(也就是类似一个表)
            // 插入1条的原因是这里的只读了一次。
            Record record = null;
            for (int count = 1; (record = lineReceiver.getFromReader()) != null; count++) {
                System.out.println(record);
                int columnNums = record.getColumnNumber();
                // 先实现一条条插入
                String device = "root.test.device2";
                List<String> measurements = new ArrayList<>(); // TODO 这个好像没传过来。
                List<TSDataType> types = new ArrayList<>();
                // List<Object> values = new ArrayList<>();
                List<String> values = new ArrayList<>();
                for (int i = 0; i < columnNums; i++) {
                    measurements.add("ss" + i);  // 没传过来先用这个
                    Column column = record.getColumn(i);
                    // values.add(column.getRawData());
                    values.add(column.getRawData().toString());
                    // TODO 需要测试一下
                    switch (column.getType()) {
                        case BOOL:
                            types.add(TSDataType.BOOLEAN);
                            break;
                        case INT:
                            types.add(TSDataType.INT32);
                            break;
                        case LONG:
                            types.add(TSDataType.INT64);
                            break;
                        case DOUBLE:
                            types.add(TSDataType.DOUBLE);
                            break;
                        case STRING:
                            types.add(TSDataType.STRING);
                            break;
                        case DATE:
                            types.add(TSDataType.DATE);
                            break;
                        default:
                            throw new RuntimeException("unsupported type:" + column.getType());
                    }
                }
                long time = System.currentTimeMillis();
                try {

                    // // 创建测点时间序列
                    // session.createMultiTimeseries(
                    //         paths, tsDataTypes, tsEncodings, compressionTypes, null, null, attributesList, null);

                    // 这个插入失败(报错)
                    // WARN  o.a.i.d.u.ErrorHandlingUtils:65 -
                    // Status code: EXECUTE_STATEMENT_ERROR(301), operation: insertRecord failed
                    // java.lang.ArrayIndexOutOfBoundsException: 11
                    // session.insertRecord(device, time, measurements, types, values);
                    // 这个插入成功，record读取一次只有一条数据，需要循环读取。
                    session.insertRecord(device, time, measurements,values);
                } catch (IoTDBConnectionException | StatementExecutionException e) {
                    throw new RuntimeException(e);
                }

                // TODO 构建List，批量写入
                // session.insertRecordsOfOneDevice(device, timestamps, measurementsList, typesList, valuesList);

            }

        }
    }
}
