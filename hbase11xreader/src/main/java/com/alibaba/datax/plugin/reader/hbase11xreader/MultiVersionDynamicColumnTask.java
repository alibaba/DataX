package com.alibaba.datax.plugin.reader.hbase11xreader;

import com.alibaba.datax.common.util.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

public class MultiVersionDynamicColumnTask extends MultiVersionTask {
    private List<String> columnFamilies = null;

    public MultiVersionDynamicColumnTask(Configuration configuration){
        super(configuration);

        this.columnFamilies = configuration.getList(Key.COLUMN_FAMILY, String.class);
    }

    @Override
    public void initScan(Scan scan) {
        for (String columnFamily : columnFamilies) {
            scan.addFamily(Bytes.toBytes(columnFamily.trim()));
        }

        super.setMaxVersions(scan);
    }
}
