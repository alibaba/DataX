/*
 * Copyright (c) 2021 OceanBase ob-loader-dumper is licensed under Mulan PSL v2. You can use this software according to
 * the terms and conditions of the Mulan PSL v2. You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING
 * BUT NOT LIMITED TO NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the Mulan PSL v2 for more
 * details.
 */
package com.alibaba.datax.plugin.writer.obhbasewriter.ext;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.writer.obhbasewriter.Hbase094xWriterErrorCode;
import com.alipay.oceanbase.hbase.OHTable;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author cjyyz
 * @date 2023/03/16
 * @since
 */
public class ObHbaseTableHolder {
    private static final Logger LOG = LoggerFactory.getLogger(ObHbaseTableHolder.class);

    private Configuration configuration;

    private String hbaseTableName;

    private OHTable ohTable;

    public ObHbaseTableHolder(Configuration configuration, String hbaseTableName) {
        this.configuration = configuration;
        this.hbaseTableName = hbaseTableName;
    }

    public OHTable getOhTable() {
        try {
            if (ohTable == null) {
                ohTable = new OHTable(configuration, hbaseTableName);
            }
            return ohTable;
        } catch (Exception e) {
            LOG.error("build obHTable: {} failed. reason: {}", hbaseTableName, e.getMessage());
            throw DataXException.asDataXException(Hbase094xWriterErrorCode.GET_HBASE_TABLE_ERROR, Hbase094xWriterErrorCode.GET_HBASE_TABLE_ERROR.getDescription());
        }
    }

    public void destroy() {
        try {
            if (ohTable != null) {
                ohTable.close();
            }
        } catch (Exception e) {
            LOG.warn("error in closing htable: {}. Reason: {}", hbaseTableName, e.getMessage());
        }
    }
}