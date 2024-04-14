package com.alibaba.datax.plugin.writer.otswriter.callable;

import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.model.DescribeTableRequest;
import com.alicloud.openservices.tablestore.model.DescribeTableResponse;
import com.alicloud.openservices.tablestore.model.TableMeta;

import java.util.concurrent.Callable;

public class GetTableMetaCallable implements Callable<TableMeta>{

    private SyncClientInterface ots = null;
    private String tableName = null;
    
    public GetTableMetaCallable(SyncClientInterface ots, String tableName) {
        this.ots = ots;
        this.tableName = tableName;
    }
    
    @Override
    public TableMeta call() throws Exception {
        DescribeTableRequest describeTableRequest = new DescribeTableRequest(tableName);
        DescribeTableResponse result = ots.describeTable(describeTableRequest);
        return result.getTableMeta();
    }

}
