package com.alibaba.datax.plugin.reader.hbase094xreader;

import com.alibaba.datax.common.util.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;

public class MultiVersionFixedColumnTask extends MultiVersionTask {

    public MultiVersionFixedColumnTask(Configuration configuration) {
        super(configuration);
    }

    @Override
    public void initScan(Scan scan) {
        for (Map<String, String> aColumn : column) {
            String columnName = aColumn.get(Key.NAME);
            if(!Hbase094xHelper.isRowkeyColumn(columnName)){
                String[] cfAndQualifier = columnName.split(":");
                scan.addColumn(Bytes.toBytes(cfAndQualifier[0].trim()), Bytes.toBytes(cfAndQualifier[1].trim()));
            }
        }
        super.setMaxVersions(scan);
    }
}
