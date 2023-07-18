package com.alibaba.datax.plugin.writer.neo4jwriter;


import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.RetryUtil;
import com.alibaba.datax.plugin.writer.neo4jwriter.adapter.DateAdapter;
import com.alibaba.datax.plugin.writer.neo4jwriter.adapter.ValueAdapter;
import com.alibaba.datax.plugin.writer.neo4jwriter.config.Neo4jProperty;
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
        List<Neo4jProperty> neo4jProperties = JSON.parseArray(config.getString(NEO4J_PROPERTIES.getKey()), Neo4jProperty.class);
        int batchSize = config.getInt(BATCH_SIZE.getKey(), BATCH_SIZE.getDefaultValue());
        int retryTimes = config.getInt(RETRY_TIMES.getKey(), RETRY_TIMES.getDefaultValue());

        return new Neo4jClient(driver,
                new WriteConfig(cypher, database, batchVariableName, neo4jProperties, batchSize),
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
        Long maxConnTime = config.getLong(MAX_CONNECTION_TIMEOUT_SECONDS.getKey(), MAX_TRANSACTION_RETRY_TIME.getDefaultValue());
        configBuilder
                .withConnectionAcquisitionTimeout(
                        maxConnTime * 2, TimeUnit.SECONDS)
                .withConnectionTimeout(maxConnTime, TimeUnit.SECONDS);


        //transaction timeout
        //事务运行超时时间
        Long txRetryTime = config.getLong(MAX_TRANSACTION_RETRY_TIME.getKey(), MAX_TRANSACTION_RETRY_TIME.getDefaultValue());
        configBuilder.withMaxTransactionRetryTime(txRetryTime, TimeUnit.SECONDS);
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
        DateAdapter.destroy();
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
//        LOGGER.debug("query:{}", query.text());
//        LOGGER.debug("batch:{}", toUnwindStr(values));
        try {
            RetryUtil.executeWithRetry(() -> {
                        session.writeTransaction(tx -> tx.run(query));
                        return null;
                    }, this.retryConfig.retryTimes, retryConfig.retrySleepMills, true,
                    Collections.singletonList(Neo4jException.class));
        } catch (Exception e) {
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
        List<Neo4jProperty> neo4jProperties = writeConfig.neo4jProperties;
        if (neo4jProperties == null || neo4jProperties.size() != sourceColNum) {
            throw new DataXException(Neo4jErrorCode.CONFIG_INVALID, "the read and write columns do not match!");
        }
        Map<String, Value> data = new HashMap<>(sourceColNum * 4 / 3);
        for (int i = 0; i < sourceColNum; i++) {
            Column column = record.getColumn(i);
            Neo4jProperty neo4jProperty = neo4jProperties.get(i);
            try {

                Value value = ValueAdapter.column2Value(column, neo4jProperty);
                data.put(neo4jProperty.getName(), value);
            } catch (Exception e) {
                LOGGER.info("dirty record：{},message :{}", column, e.getMessage());
                this.taskPluginCollector.collectDirtyRecord(record, e.getMessage());
            }
        }
        return new MapValue(data);
    }

    public List<Neo4jProperty> getNeo4jFields() {
        return this.writeConfig.neo4jProperties;
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

        List<Neo4jProperty> neo4jProperties;

        int batchSize;

        public WriteConfig(String cypher,
                           String database,
                           String batchVariableName,
                           List<Neo4jProperty> neo4jProperties,
                           int batchSize) {
            this.cypher = cypher;
            this.database = database;
            this.batchVariableName = batchVariableName;
            this.neo4jProperties = neo4jProperties;
            this.batchSize = batchSize;
        }


    }
}
