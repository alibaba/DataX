package com.alibaba.datax.plugin.reader.obhbasereader;

import com.alipay.oceanbase.hbase.OHTable;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public final class HTableManager {

    public static OHTable createHTable(Configuration config, String tableName) throws IOException {
        return new OHTable(config, tableName);
    }

    public static void closeHTable(OHTable hTable) throws IOException {
        if (hTable != null) {
            hTable.close();
        }
    }
}
