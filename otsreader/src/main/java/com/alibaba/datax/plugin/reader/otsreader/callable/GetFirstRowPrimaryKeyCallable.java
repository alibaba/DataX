package com.alibaba.datax.plugin.reader.otsreader.callable;

import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class GetFirstRowPrimaryKeyCallable implements Callable<List<PrimaryKeyColumn>> {

    private SyncClientInterface ots = null;
    private TableMeta meta = null;
    private RangeRowQueryCriteria criteria = null;

    public GetFirstRowPrimaryKeyCallable(SyncClientInterface ots, TableMeta meta, RangeRowQueryCriteria criteria) {
        this.ots = ots;
        this.meta = meta;
        this.criteria = criteria;
    }

    @Override
    public List<PrimaryKeyColumn> call() throws Exception {
        List<PrimaryKeyColumn> ret = new ArrayList<>();
        GetRangeRequest request = new GetRangeRequest();
        request.setRangeRowQueryCriteria(criteria);
        GetRangeResponse response = ots.getRange(request);
        List<Row> rows = response.getRows();
        if (rows.isEmpty()) {
            return null;// no data
        }
        Row row = rows.get(0);

        Map<String, PrimaryKeyType> pk = meta.getPrimaryKeyMap();

        for (String key : pk.keySet()) {
            PrimaryKeyColumn v = row.getPrimaryKey().getPrimaryKeyColumnsMap().get(key);
            ret.add(v);
        }
        return ret;
    }

}
