package com.alibaba.datax.plugin.reader.iotdbreader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.ErrorCode;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class IoTDBReader extends Reader {
    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private Configuration originalConfig;

        /**
         * Job对象初始化工作
         */
        @Override
        public void init() {
            // TODO 配置文件还没规划格式
            // 通过super.getPluginJobConf()获取与本插件相关的配置。
            // 读插件获得配置中reader部分，写插件获得writer部分。
            this.originalConfig = super.getPluginJobConf();
            // TODO 检查各种参数是否正确
        }

        /**
         * Job对象自身的销毁工作。
         */
        @Override
        public void destroy() {

        }

        /**
         * 全局的后置工作，比如mysqlwriter同步完影子表后的rename操作。
         */
        @Override
        public void post() {

        }

        /**
         * 将Job拆分Task。
         * @param adviceNumber 框架建议的拆分数，一般是运行时所配置的并发度。
         * @return Task的配置列表。
         */
        @Override
        public List<Configuration> split(int adviceNumber) {
            // TODO 暂时拆分为adviceNumber个，不知道是怎么切割的。。。后序需要继续测试
            // TODO DEBUG看看是不是一个配置对应一个Task，一个Task启动配置文件中的连接，执行一个查询。
            // 本机增加100个配置文件，写入txt，生成100个txt文件，运行如下
            //任务启动时刻                    : 2024-06-19 16:21:13
            // 任务结束时刻                    : 2024-06-19 16:21:24
            // 任务总计耗时                    :                 10s
            // 任务平均流量                    :           42.93MB/s
            // 记录写入速度                    :          90010rec/s
            // 读出记录总数                    :              900100
            // 读写失败总数                    :                   0
            List<Configuration> configurations = new ArrayList<>();
            for (int i = 0; i < 100; i++){
                configurations.add(this.originalConfig);
            }
            return configurations;
        }
    }

    public static class Task extends Reader.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration readerSliceConfig;
        private String mandatoryEncoding;

        private Session session;

        @Override
        public void init() {
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
        public void startRead(RecordSender recordSender) {
            try {
                // TODO 把流程调通后把SQL语句抽出去
                // SessionDataSet dataSet = session.executeQueryStatement("select * from root.sg1.d1");
                SessionDataSet dataSet = session.executeQueryStatement("select * from root.cgn.device");
                // System.out.println(dataSet.getColumnNames());
                List<String> columnTypes = dataSet.getColumnTypes();
                // System.out.println(columnTypes);
                int columnNums = columnTypes.size();
                // dataSet.setFetchSize(1024);
                while (dataSet.hasNext()) {
                    RowRecord rowRecord = dataSet.next();
                    // 将iotdb中的行 转为datax中的record
                    Record record = recordSender.createRecord();
                    // time列直接处理
                    long timestamp = rowRecord.getTimestamp();
                    record.addColumn(new LongColumn(timestamp));
                    List<Field> fields = rowRecord.getFields();
                    // 其他列遍历类型后转换
                    for (Field field : fields) {
                        TSDataType dataType = field.getDataType();
                        // null类型暂时转为字符串
                        if (dataType == null) {
                            record.addColumn(new StringColumn("null"));
                            continue;
                        }
                        switch (dataType) {
                            // TODO 把所有数据类型都测一遍
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
                                System.out.println("类型错误"+field.getDataType());
                        }
                    }
                    recordSender.sendToWriter(record);
                }
            } catch (StatementExecutionException | IoTDBConnectionException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
