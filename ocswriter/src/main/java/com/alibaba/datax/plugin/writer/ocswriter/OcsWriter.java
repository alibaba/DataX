package com.alibaba.datax.plugin.writer.ocswriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.RetryUtil;
import com.alibaba.datax.plugin.writer.ocswriter.utils.ConfigurationChecker;
import com.alibaba.datax.plugin.writer.ocswriter.utils.OcsWriterErrorCode;
import com.google.common.annotations.VisibleForTesting;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.auth.AuthDescriptor;
import net.spy.memcached.auth.PlainCallbackHandler;
import net.spy.memcached.internal.OperationFuture;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class OcsWriter extends Writer {

    public static class Job extends Writer.Job {
        private Configuration configuration;

        @Override
        public void init() {
            this.configuration = super.getPluginJobConf();
            //参数有效性检查
            ConfigurationChecker.check(this.configuration);
        }

        @Override
        public void prepare() {
            super.prepare();
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            ArrayList<Configuration> configList = new ArrayList<Configuration>();
            for (int i = 0; i < mandatoryNumber; i++) {
                configList.add(this.configuration.clone());
            }
            return configList;
        }

        @Override
        public void destroy() {
        }
    }

    public static class Task extends Writer.Task {

        private Configuration configuration;
        private MemcachedClient client;
        private Set<Integer> indexesFromUser = new HashSet<Integer>();
        private String delimiter;
        private int expireTime;
        //private int batchSize;
        private ConfigurationChecker.WRITE_MODE writeMode;
        private TaskPluginCollector taskPluginCollector;

        @Override
        public void init() {
            this.configuration = this.getPluginJobConf();
            this.taskPluginCollector = super.getTaskPluginCollector();
        }

        @Override
        public void prepare() {
            super.prepare();

            //如果用户不配置，默认为第0列
            String indexStr = this.configuration.getString(Key.INDEXES, "0");
            for (String index : indexStr.split(",")) {
                indexesFromUser.add(Integer.parseInt(index));
            }

            //如果用户不配置，默认为\u0001
            delimiter = this.configuration.getString(Key.FIELD_DELIMITER, "\u0001");
            expireTime = this.configuration.getInt(Key.EXPIRE_TIME, 0);
            //todo 此版本不支持批量提交，待ocswriter发布新版本client后支持。batchSize = this.configuration.getInt(Key.BATCH_SIZE, 100);
            writeMode = ConfigurationChecker.WRITE_MODE.valueOf(this.configuration.getString(Key.WRITE_MODE));

            String proxy = this.configuration.getString(Key.PROXY);
            //默认端口为11211
            String port = this.configuration.getString(Key.PORT, "11211");
            String username = this.configuration.getString(Key.USER);
            String password = this.configuration.getString(Key.PASSWORD);
            AuthDescriptor ad = new AuthDescriptor(new String[]{"PLAIN"}, new PlainCallbackHandler(username, password));

            try {
                client = getMemcachedConn(proxy, port, ad);
            } catch (Exception e) {
                //异常不能吃掉，直接抛出，便于定位
                throw DataXException.asDataXException(OcsWriterErrorCode.OCS_INIT_ERROR, String.format("初始化ocs客户端失败"), e);
            }
        }

        /**
         * 建立ocs客户端连接
         * 重试9次，间隔时间指数增长
         */
        private MemcachedClient getMemcachedConn(final String proxy, final String port, final AuthDescriptor ad) throws Exception {
            return RetryUtil.executeWithRetry(new Callable<MemcachedClient>() {
                @Override
                public MemcachedClient call() throws Exception {
                    return new MemcachedClient(
                            new ConnectionFactoryBuilder().setProtocol(ConnectionFactoryBuilder.Protocol.BINARY)
                                    .setAuthDescriptor(ad)
                                    .build(),
                            AddrUtil.getAddresses(proxy + ":" + port));
                }
            }, 9, 1000L, true);
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            Record record;
            String key;
            String value;
            while ((record = lineReceiver.getFromReader()) != null) {
                try {
                    key = buildKey(record);
                    value = buildValue(record);
                    switch (writeMode) {
                        case set:
                        case replace:
                        case add:
                            commitWithRetry(key, value);
                            break;
                        case append:
                        case prepend:
                            commit(key, value);
                            break;
                        default:
                            //没有default，因为参数检查的时候已经判断，不可能出现5中模式之外的模式
                    }
                } catch (Exception e) {
                    this.taskPluginCollector.collectDirtyRecord(record, e);
                }
            }
        }

        /**
         * 没有重试的commit
         */
        private void commit(final String key, final String value) {
            OperationFuture<Boolean> future;
            switch (writeMode) {
                case set:
                    future = client.set(key, expireTime, value);
                    break;
                case add:
                    //幂等原则：相同的输入得到相同的输出，不管调用多少次。
                    //所以add和replace是幂等的。
                    future = client.add(key, expireTime, value);
                    break;
                case replace:
                    future = client.replace(key, expireTime, value);
                    break;
                //todo 【注意】append和prepend重跑任务不能支持幂等，使用需谨慎，不需要重试
                case append:
                    future = client.append(0L, key, value);
                    break;
                case prepend:
                    future = client.prepend(0L, key, value);
                    break;
                default:
                    throw DataXException.asDataXException(OcsWriterErrorCode.DIRTY_RECORD, String.format("不支持的写入模式%s", writeMode.toString()));
                    //因为前面参数校验的时候已经判断，不可能存在5中操作之外的类型。
            }
            //【注意】getStatus()返回为null有可能是因为get()超时导致，此种情况当做脏数据处理。但有可能数据已经成功写入ocs。
            if (future == null || future.getStatus() == null || !future.getStatus().isSuccess()) {
                throw DataXException.asDataXException(OcsWriterErrorCode.COMMIT_FAILED, "提交数据到ocs失败");
            }
        }

        /**
         * 提交数据到ocs，有重试机制
         */
        private void commitWithRetry(final String key, final String value) throws Exception {
            RetryUtil.executeWithRetry(new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    commit(key, value);
                    return null;
                }
            }, 3, 1000L, false);
        }

        /**
         * 构建value
         * 如果有二进制字段当做脏数据处理
         * 如果col为null，当做脏数据处理
         */
        private String buildValue(Record record) {
            ArrayList<String> valueList = new ArrayList<String>();
            int colNum = record.getColumnNumber();
            for (int i = 0; i < colNum; i++) {
                Column col = record.getColumn(i);
                if (col != null) {
                    String value;
                    Column.Type type = col.getType();
                    switch (type) {
                        case STRING:
                        case BOOL:
                        case DOUBLE:
                        case LONG:
                        case DATE:
                            value = col.asString();
                            //【注意】value字段中如果有分隔符，当做脏数据处理
                            if (value != null && value.contains(delimiter)) {
                                throw DataXException.asDataXException(OcsWriterErrorCode.DIRTY_RECORD, String.format("数据中包含分隔符:%s", value));
                            }
                            break;
                        default:
                            //目前不支持二进制，如果遇到二进制，则当做脏数据处理
                            throw DataXException.asDataXException(OcsWriterErrorCode.DIRTY_RECORD, String.format("不支持的数据格式:%s", type.toString()));
                    }
                    valueList.add(value);
                } else {
                    //如果取到的列为null,需要当做脏数据处理
                    throw DataXException.asDataXException(OcsWriterErrorCode.DIRTY_RECORD, String.format("record中不存在第%s个字段", i));
                }
            }
            return StringUtils.join(valueList, delimiter);
        }

        /**
         * 构建key
         * 构建数据为空时当做脏数据处理
         */
        private String buildKey(Record record) {
            ArrayList<String> keyList = new ArrayList<String>();
            for (int index : indexesFromUser) {
                Column col = record.getColumn(index);
                if (col == null) {
                    throw DataXException.asDataXException(OcsWriterErrorCode.DIRTY_RECORD, String.format("不存在第%s列", index));
                }
                Column.Type type = col.getType();
                String value;
                switch (type) {
                    case STRING:
                    case BOOL:
                    case DOUBLE:
                    case LONG:
                    case DATE:
                        value = col.asString();
                        if (value != null && value.contains(delimiter)) {
                            throw DataXException.asDataXException(OcsWriterErrorCode.DIRTY_RECORD, String.format("主键中包含分隔符:%s", value));
                        }
                        keyList.add(value);
                        break;
                    default:
                        //目前不支持二进制，如果遇到二进制，则当做脏数据处理
                        throw DataXException.asDataXException(OcsWriterErrorCode.DIRTY_RECORD, String.format("不支持的数据格式:%s", type.toString()));
                }
            }
            String rtn = StringUtils.join(keyList, delimiter);
            if (StringUtils.isBlank(rtn)) {
                throw DataXException.asDataXException(OcsWriterErrorCode.DIRTY_RECORD, String.format("构建主键为空，请检查indexes的配置"));
            }
            return rtn;
        }

        /**
         * shutdown中会有数据异步提交，需要重试。
         */
        @Override
        public void destroy() {
            try {
                RetryUtil.executeWithRetry(new Callable<Object>() {
                    @Override
                    public Object call() throws Exception {
                        if (client == null || client.shutdown(10000L, TimeUnit.MILLISECONDS)) {
                            return null;
                        } else {
                            throw DataXException.asDataXException(OcsWriterErrorCode.SHUTDOWN_FAILED, "关闭ocsClient失败");
                        }
                    }
                }, 8, 1000L, true);
            } catch (Exception e) {
                throw DataXException.asDataXException(OcsWriterErrorCode.SHUTDOWN_FAILED, "关闭ocsClient失败", e);
            }
        }

        /**
         * 以下为测试使用
         */
        @VisibleForTesting
        public String buildValue_test(Record record) {
            return this.buildValue(record);
        }

        @VisibleForTesting
        public String buildKey_test(Record record) {
            return this.buildKey(record);
        }

        @VisibleForTesting
        public void setIndexesFromUser(HashSet<Integer> indexesFromUser) {
            this.indexesFromUser = indexesFromUser;
        }

    }
}
