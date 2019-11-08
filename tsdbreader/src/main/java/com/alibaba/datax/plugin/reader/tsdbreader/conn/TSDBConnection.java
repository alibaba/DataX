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

    public TSDBConnection(String address) {
        this.address = address;
    }

    @Override
    public String address() {
        return address;
    }

    @Override
    public String version() {
        return TSDBUtils.version(address);
    }

    @Override
    public String config() {
        return TSDBUtils.config(address);
    }

    @Override
    public String[] getSupportVersionPrefix() {
        return new String[]{"2.4", "2.5"};
    }

    @Override
    public void sendDPs(String metric, Map<String, String> tags, Long start, Long end, RecordSender recordSender) throws Exception {
        TSDBDump.dump4TSDB(this, metric, tags, start, end, recordSender);
    }

    @Override
    public void sendDPs(String metric, List<String> fields, Map<String, String> tags, Long start, Long end, RecordSender recordSender) throws Exception {
        TSDBDump.dump4TSDB(this, metric, fields, tags, start, end, recordSender);
    }

    @Override
    public void sendRecords(String metric, Map<String, String> tags, Long start, Long end, List<String> columns4RDB, RecordSender recordSender) throws Exception {
        TSDBDump.dump4RDB(this, metric, tags, start, end, columns4RDB, recordSender);
    }

    @Override
    public void sendRecords(String metric, List<String> fields, Map<String, String> tags, Long start, Long end, List<String> columns4RDB, RecordSender recordSender) throws Exception {
        TSDBDump.dump4RDB(this, metric, fields, tags, start, end, columns4RDB, recordSender);
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
