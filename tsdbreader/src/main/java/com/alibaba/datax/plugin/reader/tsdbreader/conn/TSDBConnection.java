package com.alibaba.datax.plugin.reader.tsdbreader.conn;

import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.plugin.reader.tsdbreader.util.TSDBUtils;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * Copyright @ 2019 alibaba.com
 * All right reserved.
 * Function：TSDB Connection
 *
 * @author Benedict Jin
 * @since 2019-10-21
 */
public class TSDBConnection implements Connection4TSDB {

    private String address;
    private String username;
    private String password;

    public TSDBConnection(String address, String username, String password) {
        this.address = address;
        this.username = username;
        this.password = password;
    }

    @Override
    public String address() {
        return address;
    }

    @Override
    public String username() {
        return username;
    }

    @Override
    public String password() {
        return password;
    }

    @Override
    public String version() {
        return TSDBUtils.version(address, username, password);
    }

    @Override
    public String config() {
        return TSDBUtils.config(address, username, password);
    }

    @Override
    public String[] getSupportVersionPrefix() {
        return new String[]{"2.4", "2.5"};
    }

    @Override
    public void sendDPs(String metric, Map<String, String> tags, Long start, Long end, RecordSender recordSender, Map<String, Object> hint) throws Exception {
        TSDBDump.dump4TSDB(this, metric, tags, start, end, recordSender, hint);
    }

    @Override
    public void sendDPs(String metric, List<String> fields, Map<String, String> tags, Long start, Long end, RecordSender recordSender, Map<String, Object> hint) throws Exception {
        TSDBDump.dump4TSDB(this, metric, fields, tags, start, end, recordSender, hint);
    }

    @Override
    public void sendRecords(String metric, Map<String, String> tags, Long start, Long end, List<String> columns4RDB, Boolean isCombine, RecordSender recordSender, Map<String, Object> hint) throws Exception {
        TSDBDump.dump4RDB(this, metric, tags, start, end, columns4RDB, recordSender, hint);
    }

    @Override
    public void sendRecords(List<String> metrics, Map<String, String> tags, Long start, Long end, List<String> columns4RDB, RecordSender recordSender, Map<String, Object> hint) throws Exception {
        TSDBDump.dump4RDB(this, metrics, tags, start, end, columns4RDB, recordSender, hint);
    }

    @Override
    public void sendRecords(String metric, List<String> fields, Map<String, String> tags, Long start, Long end, List<String> columns4RDB, RecordSender recordSender, Map<String, Object> hint) throws Exception {
        TSDBDump.dump4RDB(this, metric, fields, tags, start, end, columns4RDB, recordSender, hint);
    }

    @Override
    public boolean put(DataPoint4TSDB dp) {
        return false;
    }

    @Override
    public boolean put(List<DataPoint4TSDB> dps) {
        return false;
    }

    @Override
    public boolean isSupported() {
        String versionJson = version();
        if (StringUtils.isBlank(versionJson)) {
            throw new RuntimeException("Cannot get the version!");
        }
        String version = JSON.parseObject(versionJson).getString("version");
        if (StringUtils.isBlank(version)) {
            return false;
        }
        for (String prefix : getSupportVersionPrefix()) {
            if (version.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }
}
