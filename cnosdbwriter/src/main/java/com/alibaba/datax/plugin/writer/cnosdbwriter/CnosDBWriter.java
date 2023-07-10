package com.alibaba.datax.plugin.writer.cnosdbwriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.HttpClientUtil;
import com.alibaba.datax.core.util.SecretUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.writer.cnosdbwriter.format.ICnosDBRequestBuilder;
import com.alibaba.datax.plugin.writer.cnosdbwriter.format.datax.CnosDBDataXRequestBuilder;
import com.alibaba.datax.plugin.writer.cnosdbwriter.format.opentsdb.CnosDBOpenTSDBRequestBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CnosDBWriter extends Writer {
    public final static String CFG_CNOSDB_WRITE_API = "cnosdbWriteAPI";
    public final static String CFG_TENANT = "tenant";
    public final static String CFG_DATABASE = "database";
    public final static String CFG_USERNAME = "username";
    public final static String CFG_PASSWORD = "password";
    public final static String CFG_BATCH_SIZE = "batchSize";
    public final static String CFG_BUFFER_SIZE = "bufferSize";
    public final static String CFG_FORMAT = "format";
    public final static String CFG_TABLE = "table";
    public final static String CFG_TAGS = "tags";
    public final static String CFG_FIELDS = "fields";
    public final static String CFG_TIME_INDEX = "timeIndex";
    public final static String CFG_PRECISION = "precision";

    final static String ERR_MISSING_CFG_FIELD = "缺少必填的配置项: '%s'";
    final static String ERR_INVALID_CFG_PRECISION = "配置项不正确: 'precision': %s, 配置项值仅能为以下值: ms, us, ns";
    final static String ERR_INVALID_CFG_FORMAT = "配置项不正确: 'format': %s, 配置项仅能为以下值: datax, opentsdb";

    public static int precisionToMultiplier(String precision) {
        if (!(precision.equals("ms") || precision.equals("us") || precision.equals("ns"))) {
            throw new DataXException(String.format(ERR_INVALID_CFG_PRECISION, precision));
        }
        switch (precision) {
            case "us":
                // 1us = 1_000ns
                return 1_000;
            case "ns":
                return 1;
            default:
                // 1ms = 1_000_000ns
                return 1_000_000;
        }
    }

    public static class Job extends Writer.Job {
        private static final Logger LOGGER = LoggerFactory.getLogger(Job.class);

        private Configuration config = null;

        @Override
        public void init() {
            this.config = super.getPluginJobConf();
        }

        @Override
        public void destroy() {
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            LOGGER.info("拆分Job至 {} 个Task.", mandatoryNumber);
            List<Configuration> configurations = new ArrayList<>(mandatoryNumber);
            for (int i = 0; i < mandatoryNumber; i++) {
                configurations.add(this.config.clone());
            }
            return configurations;
        }
    }

    public static class Task extends Writer.Task {
        private static final Logger LOGGER = LoggerFactory.getLogger(Task.class);
        // Config cnosdbWriteAPI
        protected String cnosdbWriteAPI = "http://127.0.0.1:8902/api/v1/write";
        // Config tenant
        protected String tenant = "cnosdb";
        // Config database
        protected String database = "public";
        // Config username
        protected String username = "root";
        // Config password
        protected String password = "root";
        // Config batchSize
        protected int batchSize = 1000;
        // Config bufferSize
        protected int bufferSize = 1024 * 1024 * 8;
        // Config format
        protected String format = "datax";
        // Config table
        protected String table;
        // Config tags, but reversed key&value pair.
        protected HashMap<Integer, String> tagIndexes;
        // Config fields, but reversed key&value pair.
        protected HashMap<Integer, String> fieldIndexes;
        // Config timeIndex
        protected Integer timeIndex;
        // Config precision
        protected String precision = "ms";
        // Precision multiplier, maps from precision:
        // s -> 1,
        // ms -> 1_000,
        // us -> 1_000_000,
        // ns -> 1_000_000_000,
        protected int precisionMultiplier = 1_000;

        // The final URL of write API:
        // {cnosdbWriteAPI}?precision=ns&tenant={tenant}&db={db}
        protected String writeReqUrl;
        // The final HTTP header:
        // 'Authorization: "Basic base64({username}:{password})"'
        protected String basicAuth;

        protected ICnosDBRequestBuilder reqBuilder;

        @Override
        public void init() {
            // Note: Do not call this method twice.
            Configuration config = super.getPluginJobConf();

            this.cnosdbWriteAPI = StringUtils.defaultIfBlank(config.getString(CFG_CNOSDB_WRITE_API), this.cnosdbWriteAPI);
            this.tenant = StringUtils.defaultIfBlank(config.getString(CFG_TENANT), this.tenant);
            this.database = StringUtils.defaultIfBlank(config.getString(CFG_DATABASE), this.database);
            this.username = StringUtils.defaultIfBlank(config.getString(CFG_USERNAME), this.username);
            this.password = StringUtils.defaultIfBlank(config.getString(CFG_PASSWORD), this.password);
            Integer batchSize = config.getInt(CFG_BATCH_SIZE);
            this.batchSize = (batchSize == null || batchSize < 0) ? this.batchSize : batchSize;
            Integer bufferSize = config.getInt(CFG_BUFFER_SIZE);
            this.bufferSize = (bufferSize == null || bufferSize < 0) ? this.bufferSize : bufferSize;
            this.format = StringUtils.defaultIfBlank(config.getString(CFG_FORMAT), this.format);
            this.precision = StringUtils.defaultIfBlank(config.getString(CFG_PRECISION), this.precision);
            this.precisionMultiplier = precisionToMultiplier(this.precision);

            switch (this.format) {
                case ICnosDBRequestBuilder.FORMAT_DATAX:
                    // If using normal format: datax .
                    this.table = config.getString(CFG_TABLE);
                    if (StringUtils.isBlank(this.table)) {
                        throw new DataXException(String.format(ERR_MISSING_CFG_FIELD, CFG_TABLE));
                    }

                    Map<String, Integer> tagsMap = config.getMap(CFG_TAGS, Integer.class);
                    if (tagsMap == null) {
                        throw new DataXException(String.format(ERR_MISSING_CFG_FIELD, CFG_TAGS));
                    }
                    this.tagIndexes = new HashMap<>();
                    for (Map.Entry<String, Integer> e : tagsMap.entrySet()) {
                        if (this.tagIndexes.containsKey(e.getValue())) {
                            String k0 = this.tagIndexes.get(e.getValue());
                            String k1 = e.getKey();
                            String duplicateColumnMsg = String.format("tag key '%s' 与 '%s' 的列序号定义出现重复", k0, k1);
                            LOGGER.warn("配置项 '{}' 可能出现错误, {}", CFG_TAGS, duplicateColumnMsg);
                        }
                        this.tagIndexes.put(e.getValue(), e.getKey());
                    }

                    Map<String, Integer> fieldsMap = config.getMap(CFG_FIELDS, Integer.class);
                    if (fieldsMap == null) {
                        throw new DataXException(String.format(ERR_MISSING_CFG_FIELD, CFG_FIELDS));
                    }
                    this.fieldIndexes = new HashMap<>();
                    for (Map.Entry<String, Integer> e : fieldsMap.entrySet()) {
                        if (this.fieldIndexes.containsKey(e.getValue())) {
                            String k0 = this.fieldIndexes.get(e.getValue());
                            String k1 = e.getKey();
                            String duplicateColumnMsg = String.format("field key '%s' 与 '%s' 的列序号定义出现重复", k0, k1);
                            LOGGER.warn("配置项 '{}' 可能出现错误, {}", CFG_TAGS, duplicateColumnMsg);
                        }
                        this.fieldIndexes.put(e.getValue(), e.getKey());
                    }

                    this.timeIndex = config.getInt(CFG_TIME_INDEX);
                    if (this.timeIndex == null) {
                        throw new DataXException(String.format(ERR_MISSING_CFG_FIELD, CFG_TIME_INDEX));
                    }
                    if (this.tagIndexes.containsKey(this.timeIndex)) {
                        String k0 = this.tagIndexes.get(this.timeIndex);
                        LOGGER.warn("配置项 '{}' 可能出现错误, 与 tag key '{}' 的列序号定义出现重复", CFG_TIME_INDEX, k0);
                    } else if (this.fieldIndexes.containsKey(this.timeIndex)) {
                        String k0 = this.fieldIndexes.get(this.timeIndex);
                        LOGGER.warn("配置项 '{}' 可能出现错误, 与 field key '{}' 的列序号定义出现重复", CFG_TIME_INDEX, k0);
                    }
                    this.reqBuilder = new CnosDBDataXRequestBuilder(this.bufferSize, this.batchSize, this.precisionMultiplier, this.table, this.tagIndexes, this.fieldIndexes, this.timeIndex);
                    break;
                case ICnosDBRequestBuilder.FORMAT_OPENTSDB:
                    // If using opentsdbreader format: opentsdb .
                    this.reqBuilder = new CnosDBOpenTSDBRequestBuilder(this.bufferSize, this.batchSize, this.precisionMultiplier);
                    break;
                default:
                    throw new DataXException(String.format(ERR_INVALID_CFG_FORMAT, this.format));
            }

            // Set precision=ns, the inserted timestamp is data.timestamp * precisionMultiplier
            this.writeReqUrl = this.cnosdbWriteAPI + "?precision=ns";
            if (!this.tenant.isEmpty()) {
                this.writeReqUrl = this.writeReqUrl + "&tenant=" + this.tenant;
            }
            if (!this.database.isEmpty()) {
                this.writeReqUrl = this.writeReqUrl + "&db=" + this.database;
            }
            try {
                this.basicAuth = this.getAuthorization(this.username, this.password);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private String getAuthorization(String username, String password) throws Exception {
            return "Basic " + SecretUtil.encryptBASE64((username + ":" + password).getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public void destroy() {
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            try {
                Record record;
                while ((record = lineReceiver.getFromReader()) != null) {
                    try {
                        this.reqBuilder.appendRecord(record).ifPresent(this::writeCnosDB);
                    } catch (CnosDBWriterException writerException) {
                        LOGGER.error(writerException.getMessage());
                        super.getTaskPluginCollector().collectDirtyRecord(record, writerException);
                    }
                }

                if (this.reqBuilder.length() > 0) {
                    // Write the buffer to CnosDB.
                    String lp = this.reqBuilder.take();
                    try {
                        this.writeCnosDB(lp);
                    } catch (CnosDBWriterException writerException) {
                        LOGGER.error(writerException.getMessage());
                        super.getTaskPluginCollector().collectDirtyRecord(record, writerException);
                    }
                }
            } catch (Exception e) {
                throw DataXException.asDataXException(DBUtilErrorCode.WRITE_DATA_ERROR, e);
            }
        }

        private void writeCnosDB(String lpData) {
            LOGGER.trace("sending lines: {}.", lpData);
            HttpPost req = HttpClientUtil.getPostRequest();
            req.setURI(URI.create(this.writeReqUrl));
            req.setHeader("Authorization", this.basicAuth);
            try {
                HttpEntity entity = new StringEntity(lpData);
                req.setEntity(entity);
            } catch (UnsupportedEncodingException e) {
                throw new CnosDBWriterException(CnosDBWriterErrorCode.EncodeWriteRequest, "写入请求编码失败");
            }
            try {
                HttpClientUtil.getHttpClientUtil().executeAndGetWithRetry(req, 3, 1000);
            } catch (DataXException e) {
                throw new CnosDBWriterException(CnosDBWriterErrorCode.SendWriteRequestHTTP, "写入 CnosDB 失败");
            }
        }

    }
}
