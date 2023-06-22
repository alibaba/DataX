package com.alibaba.datax.plugin.writer.neo4jwriter;


import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.RetryUtil;
import com.alibaba.datax.plugin.writer.neo4jwriter.adapter.ValueAdapter;
import com.alibaba.datax.plugin.writer.neo4jwriter.config.Neo4jField;
import com.alibaba.datax.plugin.writer.neo4jwriter.exception.Neo4jErrorCode;
import com.alibaba.fastjson2.JSON;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.driver.*;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.internal.value.MapValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.alibaba.datax.plugin.writer.neo4jwriter.config.ConfigConstants.*;
import static com.alibaba.datax.plugin.writer.neo4jwriter.exception.Neo4jErrorCode.DATABASE_ERROR;

public class Neo4jClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jClient.class);
    private Driver driver;

    private WriteConfig writeConfig;
    private RetryConfig retryConfig;
    private TaskPluginCollector taskPluginCollector;

    private Session session;

    private List<MapValue> writerBuffer;


    public Neo4jClient(Driver driver,
                       WriteConfig writeConfig,
                       RetryConfig retryConfig,
                       TaskPluginCollector taskPluginCollector) {
        this.driver = driver;
        this.writeConfig = writeConfig;
        this.retryConfig = retryConfig;
        this.taskPluginCollector = taskPluginCollector;
        this.writerBuffer = new ArrayList<>(writeConfig.batchSize);
    }

    public void init() {
        String database = writeConfig.database;
        //neo4j 3.x 没有数据库
        //neo4j 3.x no database
        if (null != database && !"".equals(database)) {
            this.session = driver.session(SessionConfig.forDatabase(database));
        } else {
            this.session = driver.session();
        }
    }

    public static Neo4jClient build(Configuration config, TaskPluginCollector taskPluginCollector) {

        Driver driver = buildNeo4jDriver(config);
        String cypher = checkCypher(config);
        String database = config.getString(DATABASE.getKey());
        String batchVariableName = config.getString(BATCH_DATA_VARIABLE_NAME.getKey(),
                BATCH_DATA_VARIABLE_NAME.getDefaultValue());
        List<Neo4jField> neo4jFields = JSON.parseArray(config.getString(NEO4J_FIELDS.getKey()), Neo4jField.class);
        int batchSize = config.getInt(BATCH_SIZE.getKey(), BATCH_SIZE.getDefaultValue());
        int retryTimes = config.getInt(RETRY_TIMES.getKey(), RETRY_TIMES.getDefaultValue());

        return new Neo4jClient(driver,
                new WriteConfig(cypher, database, batchVariableName, neo4jFields, batchSize),
                new RetryConfig(retryTimes, config.getLong(RETRY_SLEEP_MILLS.getKey(), RETRY_SLEEP_MILLS.getDefaultValue())),
                taskPluginCollector
        );
    }

    private static String checkCypher(Configuration config) {
        String cypher = config.getString(CYPHER.getKey());
        if (StringUtils.isBlank(cypher)) {
            throw DataXException.asDataXException(Neo4jErrorCode.CONFIG_INVALID, "cypher must not null or empty");
        }
        return cypher;
    }

    private static Driver buildNeo4jDriver(Configuration config) {

        Config.ConfigBuilder configBuilder = Config.builder().withMaxConnectionPoolSize(1);
        String uri = checkUriConfig(config);

        //connection timeout
        //连接超时时间
        Long maxConnTime = config.getLong(MAX_CONNECTION_TIMEOUT_SECONDS.getKey());
        if (maxConnTime != null && maxConnTime > 0) {
            configBuilder
                    .withConnectionAcquisitionTimeout(
                            maxConnTime * 2, TimeUnit.SECONDS)
                    .withConnectionTimeout(maxConnTime, TimeUnit.SECONDS);
        }

        //transaction timeout
        //事务运行超时时间
        Long txRetryTime = config.getLong(MAX_TRANSACTION_RETRY_TIME.getKey());
        if (txRetryTime != null && txRetryTime > 0) {
            configBuilder.withMaxTransactionRetryTime(
                    txRetryTime, TimeUnit.SECONDS);
        }

        String username = config.getString(USERNAME.getKey());
        String password = config.getString(PASSWORD.getKey());
        String bearerToken = config.getString(BEARER_TOKEN.getKey());
        String kerberosTicket = config.getString(KERBEROS_TICKET.getKey());

        if (StringUtils.isNotBlank(username) && StringUtils.isNotBlank(password)) {

            return GraphDatabase.driver(uri, AuthTokens.basic(username, password), configBuilder.build());

        } else if (StringUtils.isNotBlank(bearerToken)) {

            return GraphDatabase.driver(uri, AuthTokens.bearer(bearerToken), configBuilder.build());

        } else if (StringUtils.isNotBlank(kerberosTicket)) {

            return GraphDatabase.driver(uri, AuthTokens.kerberos(kerberosTicket), configBuilder.build());

        }

        throw DataXException.asDataXException(Neo4jErrorCode.CONFIG_INVALID, "Invalid Auth config.");
    }

    private static String checkUriConfig(Configuration config) {
        String uri = config.getString(URI.getKey());
        if (null == uri || uri.length() == 0) {
            throw DataXException.asDataXException(Neo4jErrorCode.CONFIG_INVALID, "Invalid uri configuration");
        }
        return uri;
    }

    public void destroy() {
        tryFlushBuffer();
        if (driver != null) {
            driver.close();
        }
        if (session != null) {
            session.close();
        }
    }

    private void tryFlushBuffer() {
        if (!writerBuffer.isEmpty()) {
            doWrite(writerBuffer);
            writerBuffer.clear();
        }
    }

    private void tryBatchWrite() {
        if (!writerBuffer.isEmpty() && writerBuffer.size() >= writeConfig.batchSize) {
            doWrite(writerBuffer);
            writerBuffer.clear();
        }
    }

    private void doWrite(List<MapValue> values) {
        Value batchValues = Values.parameters(this.writeConfig.batchVariableName, values);
        Query query = new Query(this.writeConfig.cypher, batchValues);
        LOGGER.debug("query:{}", query.text());
        LOGGER.debug("batch:{}", toUnwindStr(values));
        try {
            RetryUtil.executeWithRetry(() -> {
                        session.writeTransaction(tx -> tx.run(query));
                        return null;
                    }, this.retryConfig.retryTimes, retryConfig.retrySleepMills, true,
                    Collections.singletonList(Neo4jException.class));
        } catch (Exception e) {
            LOGGER.error("在写入数据库时发生了异常，原因是：{}", e.getMessage());
            LOGGER.error("an exception occurred while writing to the database,message:{}", e.getMessage());
            throw DataXException.asDataXException(DATABASE_ERROR, e.getMessage());
        }


    }

    private String toUnwindStr(List<MapValue> values) {
        StringJoiner joiner = new StringJoiner(",");
        for (MapValue value : values) {
            joiner.add(value.toString());
        }
        return "[" + joiner + "]";
    }

    public void tryWrite(Record record) {
        MapValue neo4jValue = checkAndConvert(record);
        writerBuffer.add(neo4jValue);
        tryBatchWrite();
    }

    private MapValue checkAndConvert(Record record) {
        int sourceColNum = record.getColumnNumber();
        List<Neo4jField> neo4jFields = writeConfig.neo4jFields;
        if (sourceColNum < neo4jFields.size()) {
            LOGGER.warn("接收到的数据列少于neo4jWriter企图消费的数据列,请注意风险，这可能导致数据不匹配");
            LOGGER.warn("Receive fewer data columns than neo4jWriter attempts to consume, " +
                    "be aware of the risk that this may result in data mismatch");
            LOGGER.warn("接受到的数据是：" + record);
            LOGGER.warn("received data is：" + record);
        }

        int len = Math.min(sourceColNum, neo4jFields.size());
        Map<String, Value> data = new HashMap<>(len * 4 / 3);
        for (int i = 0; i < len; i++) {
            Column column = record.getColumn(i);
            Neo4jField neo4jField = neo4jFields.get(i);
            try {

                Value value = ValueAdapter.column2Value(column, neo4jField);
                data.put(neo4jField.getFieldName(), value);
            } catch (Exception e) {
                LOGGER.info("检测到一条脏数据：{},原因:{}", column, e.getMessage());
                LOGGER.info("dirty record：{},message :{}", column, e.getMessage());
                this.taskPluginCollector.collectDirtyRecord(record, e.getMessage());
            }
        }
        return new MapValue(data);
    }

    public List<Neo4jField> getNeo4jFields() {
        return this.writeConfig.neo4jFields;
    }


    static class RetryConfig {
        int retryTimes;
        long retrySleepMills;

        RetryConfig(int retryTimes, long retrySleepMills) {
            this.retryTimes = retryTimes;
            this.retrySleepMills = retrySleepMills;
        }
    }

    static class WriteConfig {
        String cypher;

        String database;

        String batchVariableName;

        List<Neo4jField> neo4jFields;

        int batchSize;

        public WriteConfig(String cypher,
                           String database,
                           String batchVariableName,
                           List<Neo4jField> neo4jFields,
                           int batchSize) {
            this.cypher = cypher;
            this.database = database;
            this.batchVariableName = batchVariableName;
            this.neo4jFields = neo4jFields;
            this.batchSize = batchSize;
        }


    }
}
