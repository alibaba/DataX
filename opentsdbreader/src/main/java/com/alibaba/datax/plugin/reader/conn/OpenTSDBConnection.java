package com.alibaba.datax.plugin.reader.conn;

import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.plugin.reader.util.TSDBUtils;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * Copyright @ 2019 alibaba.com
 * All right reserved.
 * Function：OpenTSDB Connection
 *
 * @author Benedict Jin
 * @since 2019-03-29
 */
public class OpenTSDBConnection implements Connection4TSDB {

    private String address;

    public OpenTSDBConnection(String address) {
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
        return new String[]{"2.3"};
    }

    @Override
    public void sendDPs(String metric, Long start, Long end, RecordSender recordSender) throws Exception {
        OpenTSDBDump.dump(this, metric, start, end, recordSender);
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
