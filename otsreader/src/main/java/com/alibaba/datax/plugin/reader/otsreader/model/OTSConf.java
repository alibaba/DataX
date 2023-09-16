package com.alibaba.datax.plugin.reader.otsreader.model;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.otsreader.utils.Constant;
import com.alibaba.datax.plugin.reader.otsreader.utils.Key;
import com.alibaba.datax.plugin.reader.otsreader.utils.ParamChecker;
import com.alicloud.openservices.tablestore.model.ColumnType;

import java.util.List;

public class OTSConf {
    private String endpoint = null;
    private String accessId = null;
    private String accessKey = null;
    private String instanceName = null;
    private String tableName = null;
    private OTSRange range = null;
    private List<OTSColumn> column = null;
    private OTSMode mode = null;

    @Deprecated
    private String metaMode = "";

    private boolean newVersion = false;
    /**
     * 以下配置仅用于timeseries数据读取
     */
    private boolean isTimeseriesTable = false;
    private String measurementName = null;
    /**
     * 以上配置仅用于timeseries数据读取
     */
    private OTSMultiVersionConf multi = null;
    
    private int retry = Constant.ConfigDefaultValue.RETRY;
    private int retryPauseInMillisecond = Constant.ConfigDefaultValue.RETRY_PAUSE_IN_MILLISECOND;
    private int ioThreadCount = Constant.ConfigDefaultValue.IO_THREAD_COUNT;
    private int maxConnectionCount = Constant.ConfigDefaultValue.MAX_CONNECTION_COUNT;
    private int socketTimeoutInMillisecond = Constant.ConfigDefaultValue.SOCKET_TIMEOUT_IN_MILLISECOND;
    private int connectTimeoutInMillisecond = Constant.ConfigDefaultValue.CONNECT_TIMEOUT_IN_MILLISECOND;

    public int getIoThreadCount() {
        return ioThreadCount;
    }

    public void setIoThreadCount(int ioThreadCount) {
        this.ioThreadCount = ioThreadCount;
    }

    public int getMaxConnectCount() {
        return maxConnectionCount;
    }

    public void setMaxConnectCount(int maxConnectCount) {
        this.maxConnectionCount = maxConnectCount;
    }

    public int getSocketTimeoutInMillisecond() {
        return socketTimeoutInMillisecond;
    }

    public void setSocketTimeoutInMillisecond(int socketTimeoutInMillisecond) {
        this.socketTimeoutInMillisecond = socketTimeoutInMillisecond;
    }

    public int getConnectTimeoutInMillisecond() {
        return connectTimeoutInMillisecond;
    }

    public void setConnectTimeoutInMillisecond(int connectTimeoutInMillisecond) {
        this.connectTimeoutInMillisecond = connectTimeoutInMillisecond;
    }

    public int getRetry() {
        return retry;
    }

    public void setRetry(int retry) {
        this.retry = retry;
    }

    public int getRetryPauseInMillisecond() {
        return retryPauseInMillisecond;
    }

    public void setRetryPauseInMillisecond(int sleepInMillisecond) {
        this.retryPauseInMillisecond = sleepInMillisecond;
    }

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

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public OTSRange getRange() {
        return range;
    }

    public void setRange(OTSRange range) {
        this.range = range;
    }

    public OTSMode getMode() {
        return mode;
    }

    public void setMode(OTSMode mode) {
        this.mode = mode;
    }

    public OTSMultiVersionConf getMulti() {
        return multi;
    }

    public void setMulti(OTSMultiVersionConf multi) {
        this.multi = multi;
    }

    public List<OTSColumn> getColumn() {
        return column;
    }

    public void setColumn(List<OTSColumn> column) {
        this.column = column;
    }

    public boolean isNewVersion() {
        return newVersion;
    }

    public void setNewVersion(boolean newVersion) {
        this.newVersion = newVersion;
    }

    @Deprecated
    public String getMetaMode() {
        return metaMode;
    }

    @Deprecated
    public void setMetaMode(String metaMode) {
        this.metaMode = metaMode;
    }

    public boolean isTimeseriesTable() {
        return isTimeseriesTable;
    }

    public void setTimeseriesTable(boolean timeseriesTable) {
        isTimeseriesTable = timeseriesTable;
    }

    public String getMeasurementName() {
        return measurementName;
    }

    public void setMeasurementName(String measurementName) {
        this.measurementName = measurementName;
    }

    public static OTSConf load(Configuration param) throws OTSCriticalException {
        OTSConf c = new OTSConf();
        
        // account
        c.setEndpoint(ParamChecker.checkStringAndGet(param, Key.OTS_ENDPOINT, true));
        c.setAccessId(ParamChecker.checkStringAndGet(param, Key.OTS_ACCESSID, true));
        c.setAccessKey(ParamChecker.checkStringAndGet(param, Key.OTS_ACCESSKEY, true));
        c.setInstanceName(ParamChecker.checkStringAndGet(param, Key.OTS_INSTANCE_NAME, true));
        c.setTableName(ParamChecker.checkStringAndGet(param, Key.TABLE_NAME, true));
        
        c.setRetry(param.getInt(Constant.ConfigKey.RETRY, Constant.ConfigDefaultValue.RETRY));
        c.setRetryPauseInMillisecond(param.getInt(Constant.ConfigKey.RETRY_PAUSE_IN_MILLISECOND, Constant.ConfigDefaultValue.RETRY_PAUSE_IN_MILLISECOND));
        c.setIoThreadCount(param.getInt(Constant.ConfigKey.IO_THREAD_COUNT, Constant.ConfigDefaultValue.IO_THREAD_COUNT));
        c.setMaxConnectCount(param.getInt(Constant.ConfigKey.MAX_CONNECTION_COUNT, Constant.ConfigDefaultValue.MAX_CONNECTION_COUNT));
        c.setSocketTimeoutInMillisecond(param.getInt(Constant.ConfigKey.SOCKET_TIMEOUTIN_MILLISECOND, Constant.ConfigDefaultValue.SOCKET_TIMEOUT_IN_MILLISECOND));
        c.setConnectTimeoutInMillisecond(param.getInt(Constant.ConfigKey.CONNECT_TIMEOUT_IN_MILLISECOND, Constant.ConfigDefaultValue.CONNECT_TIMEOUT_IN_MILLISECOND));

        // range
        c.setRange(ParamChecker.checkRangeAndGet(param));
        
        // mode 可选参数
        c.setMode(ParamChecker.checkModeAndGet(param));
        //isNewVersion 可选参数
        c.setNewVersion(param.getBool(Key.NEW_VERSION, false));
        // metaMode 旧版本配置
        c.setMetaMode(param.getString(Key.META_MODE, ""));



        // 读时序表配置项
        c.setTimeseriesTable(param.getBool(Key.IS_TIMESERIES_TABLE, false));
        // column
        if(!c.isTimeseriesTable()){
            //非时序表
            c.setColumn(ParamChecker.checkOTSColumnAndGet(param, c.getMode()));
        }
        else{
            // 时序表
            c.setMeasurementName(param.getString(Key.MEASUREMENT_NAME, ""));
            c.setColumn(ParamChecker.checkTimeseriesColumnAndGet(param));
            ParamChecker.checkTimeseriesMode(c.getMode(), c.isNewVersion());
        }

        if (c.getMode() == OTSMode.MULTI_VERSION) {
            c.setMulti(OTSMultiVersionConf.load(param));
        } 
        return c;
    }
}
