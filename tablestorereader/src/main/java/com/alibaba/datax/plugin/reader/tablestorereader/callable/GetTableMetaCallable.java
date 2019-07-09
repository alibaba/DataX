package com.alibaba.datax.plugin.reader.tablestorereader.callable;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.DescribeTableRequest;
import com.alicloud.openservices.tablestore.model.DescribeTableResponse;
import com.alicloud.openservices.tablestore.model.TableMeta;

import java.util.concurrent.Callable;

public class GetTableMetaCallable implements Callable<TableMeta>{

    private SyncClient syncClient = null;
    private String tableName = null;

    public GetTableMetaCallable(SyncClient ots, String tableName) {
        this.syncClient = ots;
        this.tableName = tableName;
    }

    @Override
    public TableMeta call() throws Exception {

        DescribeTableRequest describeTableRequest = new DescribeTableRequest();
        describeTableRequest.setTableName(tableName);

        DescribeTableResponse describeTableResponse = syncClient.describeTable(describeTableRequest);
        return describeTableResponse.getTableMeta();
    }
}
