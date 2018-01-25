package com.alibaba.datax.plugin.reader.otsstreamreader.internal.config;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.OTSStreamReaderException;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.ParamChecker;
import com.alibaba.datax.plugin.reader.otsstreamreader.internal.utils.TimeUtils;
import com.alicloud.openservices.tablestore.SyncClientInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OTSStreamReaderConfig {

    private static final Logger LOG = LoggerFactory.getLogger(OTSStreamReaderConfig.class);

    private static final String KEY_OTS_ENDPOINT = "endpoint";
    private static final String KEY_OTS_ACCESSID = "accessId";
    private static final String KEY_OTS_ACCESSKEY = "accessKey";
    private static final String KEY_OTS_INSTANCE_NAME = "instanceName";
    private static final String KEY_DATA_TABLE_NAME = "dataTable";
    private static final String KEY_STATUS_TABLE_NAME = "statusTable";
    private static final String KEY_START_TIMESTAMP_MILLIS = "startTimestampMillis";
    private static final String KEY_END_TIMESTAMP_MILLIS = "endTimestampMillis";
    private static final String KEY_START_TIME_STRING = "startTimeString";
    private static final String KEY_END_TIME_STRING = "endTimeString";
    private static final String KEY_IS_EXPORT_SEQUENCE_INFO = "isExportSequenceInfo";
    private static final String KEY_DATE = "date";
    private static final String KEY_MAX_RETRIES = "maxRetries";
    private static final String KEY_MODE = "mode";
    private static final String KEY_COLUMN = "column";
    private static final String KEY_THREAD_NUM = "threadNum";

    private static final int DEFAULT_MAX_RETRIES = 30;
    private static final long DEFAULT_SLAVE_LOOP_INTERVAL = 10 * TimeUtils.SECOND_IN_MILLIS;
    private static final long DEFAULT_SLAVE_LOGGING_STATUS_INTERVAL = 60 * TimeUtils.SECOND_IN_MILLIS;

    private String endpoint;
    private String accessId;
    private String accessKey;
    private String instanceName;
    private String dataTable;
    private String statusTable;
    private long startTimestampMillis;
    private long endTimestampMillis;
    private boolean isExportSequenceInfo;
    private int maxRetries = DEFAULT_MAX_RETRIES;
    private int threadNum = 32;
    private long slaveLoopInterval = DEFAULT_SLAVE_LOOP_INTERVAL;
    private long slaveLoggingStatusInterval = DEFAULT_SLAVE_LOGGING_STATUS_INTERVAL;

    private Mode mode;
    private List<String> columns;

    private transient SyncClientInterface otsForTest;

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getAccessId() {
        return accessId;
    }

    public void setAccessId(String accessId) {
        this.accessId = accessId;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    public String getDataTable() {
        return dataTable;
    }

    public void setDataTable(String dataTable) {
        this.dataTable = dataTable;
    }

    public String getStatusTable() {
        return statusTable;
    }

    public void setStatusTable(String statusTable) {
        this.statusTable = statusTable;
    }

    public long getStartTimestampMillis() {
        return startTimestampMillis;
    }

    public void setStartTimestampMillis(long startTimestampMillis) {
        this.startTimestampMillis = startTimestampMillis;
    }

    public long getEndTimestampMillis() {
        return endTimestampMillis;
    }

    public void setEndTimestampMillis(long endTimestampMillis) {
        this.endTimestampMillis = endTimestampMillis;
    }

    public boolean isExportSequenceInfo() {
        return isExportSequenceInfo;
    }

    public void setIsExportSequenceInfo(boolean isExportSequenceInfo) {
        this.isExportSequenceInfo = isExportSequenceInfo;
    }

    public Mode getMode() {
        return mode;
    }

    public void setMode(Mode mode) {
        this.mode = mode;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    private static void parseConfigForSingleVersionAndUpdateOnlyMode(OTSStreamReaderConfig config, Configuration param) {
        try {
            List<Object> values = param.getList(KEY_COLUMN);
            if (values == null) {
                config.setColumns(new ArrayList<String>());
                return;
            }

            List<String> columns = new ArrayList<String>();
            for (Object item : values) {
                if (item instanceof Map) {
                    String columnName = (String) ((Map) item).get("name");
                    columns.add(columnName);
                } else {
                    throw new IllegalArgumentException("The item of column must be map object, please check your input.");
                }
            }
            config.setColumns(columns);
        } catch (RuntimeException ex) {
            throw new OTSStreamReaderException("Parse column fail, please check your config.", ex);
        }
    }

    public static OTSStreamReaderConfig load(Configuration param) {
        OTSStreamReaderConfig config = new OTSStreamReaderConfig();

        config.setEndpoint(ParamChecker.checkStringAndGet(param, KEY_OTS_ENDPOINT, true));
        config.setAccessId(ParamChecker.checkStringAndGet(param, KEY_OTS_ACCESSID, true));
        config.setAccessKey(ParamChecker.checkStringAndGet(param, KEY_OTS_ACCESSKEY, true));
        config.setInstanceName(ParamChecker.checkStringAndGet(param, KEY_OTS_INSTANCE_NAME, true));
        config.setDataTable(ParamChecker.checkStringAndGet(param, KEY_DATA_TABLE_NAME, true));
        config.setStatusTable(ParamChecker.checkStringAndGet(param, KEY_STATUS_TABLE_NAME, true));
        config.setIsExportSequenceInfo(param.getBool(KEY_IS_EXPORT_SEQUENCE_INFO, false));

        if (param.getInt(KEY_THREAD_NUM) != null) {
            config.setThreadNum(param.getInt(KEY_THREAD_NUM));
        }

        if (param.getString(KEY_DATE) == null &&
                (param.getLong(KEY_START_TIMESTAMP_MILLIS) == null || param.getLong(KEY_END_TIMESTAMP_MILLIS) == null) && 
                (param.getLong(KEY_START_TIME_STRING) == null || param.getLong(KEY_END_TIME_STRING) == null)) {
            throw new OTSStreamReaderException("Must set date or time range millis or time range string, please check your config.");
        }
        
        if (param.get(KEY_DATE) != null &&
                (param.getLong(KEY_START_TIMESTAMP_MILLIS) != null || param.getLong(KEY_END_TIMESTAMP_MILLIS) != null) &&
                (param.getLong(KEY_START_TIME_STRING) != null || param.getLong(KEY_END_TIME_STRING) != null)) {
            throw new OTSStreamReaderException("Can't set date and time range millis and time range string, please check your config.");
        }
        
        if (param.get(KEY_DATE) != null &&
                (param.getLong(KEY_START_TIMESTAMP_MILLIS) != null || param.getLong(KEY_END_TIMESTAMP_MILLIS) != null)) {
            throw new OTSStreamReaderException("Can't set date and time range both, please check your config.");
        }
        
        if (param.get(KEY_DATE) != null &&
                (param.getLong(KEY_START_TIME_STRING) != null || param.getLong(KEY_END_TIME_STRING) != null)) {
            throw new OTSStreamReaderException("Can't set date and time range string both, please check your config.");
        }
        
        if ((param.getLong(KEY_START_TIMESTAMP_MILLIS) != null || param.getLong(KEY_END_TIMESTAMP_MILLIS) != null)&&
                (param.getLong(KEY_START_TIME_STRING) != null || param.getLong(KEY_END_TIME_STRING) != null)) {
            throw new OTSStreamReaderException("Can't set time range millis and time range string both, please check your config.");
        }

        if (param.getString(KEY_START_TIME_STRING) != null &&
                param.getString(KEY_END_TIME_STRING) != null) {
            String startTime=ParamChecker.checkStringAndGet(param, KEY_START_TIME_STRING, true);
            String endTime=ParamChecker.checkStringAndGet(param, KEY_END_TIME_STRING, true);
            try {
                long startTimestampMillis = TimeUtils.parseTimeStringToTimestampMillis(startTime);
                config.setStartTimestampMillis(startTimestampMillis);
            } catch (Exception ex) {
                throw new OTSStreamReaderException("Can't parse startTimeString: " + startTime);
            }
            try {
                long endTimestampMillis = TimeUtils.parseTimeStringToTimestampMillis(endTime);
                config.setEndTimestampMillis(endTimestampMillis);
            } catch (Exception ex) {
                throw new OTSStreamReaderException("Can't parse startTimeString: " + startTime);
            }  
            
        }else if (param.getString(KEY_DATE) == null) {
            config.setStartTimestampMillis(param.getLong(KEY_START_TIMESTAMP_MILLIS));
            config.setEndTimestampMillis(param.getLong(KEY_END_TIMESTAMP_MILLIS));
        } else {
            String date = ParamChecker.checkStringAndGet(param, KEY_DATE, true);
            try {
                long startTimestampMillis = TimeUtils.parseDateToTimestampMillis(date);
                config.setStartTimestampMillis(startTimestampMillis);
                config.setEndTimestampMillis(startTimestampMillis + TimeUtils.DAY_IN_MILLIS);
            } catch (ParseException ex) {
                throw new OTSStreamReaderException("Can't parse date: " + date);
            }
        }

        


        if (config.getStartTimestampMillis() >= config.getEndTimestampMillis()) {
            throw new OTSStreamReaderException("EndTimestamp must be larger than startTimestamp.");
        }

        config.setMaxRetries(param.getInt(KEY_MAX_RETRIES, DEFAULT_MAX_RETRIES));

        String mode = param.getString(KEY_MODE);
        if (mode != null) {
            if (mode.equalsIgnoreCase(Mode.SINGLE_VERSION_AND_UPDATE_ONLY.name())) {
                config.setMode(Mode.SINGLE_VERSION_AND_UPDATE_ONLY);
                parseConfigForSingleVersionAndUpdateOnlyMode(config, param);
            } else {
                throw new OTSStreamReaderException("Unsupported Mode: " + mode + ", please check your config.");
            }
        } else {
            config.setMode(Mode.MULTI_VERSION);
            List<Object> values = param.getList(KEY_COLUMN);
            if (values != null) {
                throw new OTSStreamReaderException("The multi version mode doesn't support setting columns.");
            }
        }

        LOG.info("endpoint: {}, accessId: {}, accessKey: {}, instanceName: {}, dataTableName: {}, statusTableName: {}," +
                " isExportSequenceInfo: {}, startTimestampMillis: {}, endTimestampMillis:{}, maxRetries:{}.", config.getEndpoint(),
                config.getAccessId(), config.getAccessKey(), config.getInstanceName(), config.getDataTable(),
                config.getStatusTable(), config.isExportSequenceInfo(), config.getStartTimestampMillis(),
                config.getEndTimestampMillis(), config.getMaxRetries());

        return config;
    }

    /**
     * test use
     * @return
     */
    public SyncClientInterface getOtsForTest() {
        return otsForTest;
    }

    /**
     * test use
     * @param otsForTest
     */
    public void setOtsForTest(SyncClientInterface otsForTest) {
        this.otsForTest = otsForTest;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public int getThreadNum() {
        return threadNum;
    }

    public void setSlaveLoopInterval(long slaveLoopInterval) {
        this.slaveLoopInterval = slaveLoopInterval;
    }

    public void setSlaveLoggingStatusInterval(long slaveLoggingStatusInterval) {
        this.slaveLoggingStatusInterval = slaveLoggingStatusInterval;
    }

    public long getSlaveLoopInterval() {
        return slaveLoopInterval;
    }

    public long getSlaveLoggingStatusInterval() {
        return slaveLoggingStatusInterval;
    }

    public void setThreadNum(int threadNum) {
        this.threadNum = threadNum;
    }
}
