package com.alibaba.datax.plugin.writer.conn;

import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.plugin.writer.util.TSDBUtils;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * Copyright @ 2019 alibaba.com
 * All right reserved.
 * Function：TSDB Connection
 *
 * @author Benedict Jin
 * @since 2019-03-29
 */
public class TSDBConnection implements Connection4TSDB {

    private String address;

    public TSDBConnection(String address) {
        if (StringUtils.isBlank(address)) {
            throw new RuntimeException("TSDBConnection init failed because address is blank!");
        }
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
        return new String[]{"2.4.1", "2.4.2"};
    }

    @Override
    public void sendDPs(String metric, Long start, Long end, RecordSender recordSender) {
        throw new RuntimeException("Not support yet!");
    }

    @Override
    public boolean put(DataPoint4TSDB dp) {
        return TSDBUtils.put(address, dp);
    }

    @Override
    public boolean put(List<DataPoint4TSDB> dps) {
        return TSDBUtils.put(address, dps);
    }

    @Override
    public boolean put(String dps) {
        return TSDBUtils.put(address, dps);
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
