package com.alibaba.datax.plugin.reader.conn;

import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.plugin.reader.util.TSDBUtils;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

//This file is part of OpenTSDB.

//Copyright (C) 2010-2012  The OpenTSDB Authors.
//Copyright（C）2019 Alibaba Group Holding Ltd.

//

//This program is free software: you can redistribute it and/or modify it

//under the terms of the GNU Lesser General Public License as published by

//the Free Software Foundation, either version 2.1 of the License, or (at your

//option) any later version.  This program is distributed in the hope that it

//will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty

//of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser

//General Public License for more details.  You should have received a copy

//of the GNU Lesser General Public License along with this program.  If not,

//see <http://www.gnu.org/licenses/>.
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
