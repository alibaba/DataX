package com.alibaba.datax.plugin.reader.otsreader.callable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import com.aliyun.openservices.ots.OTSClient;
import com.aliyun.openservices.ots.model.ColumnType;
import com.aliyun.openservices.ots.model.ColumnValue;
import com.aliyun.openservices.ots.model.GetRangeRequest;
import com.aliyun.openservices.ots.model.GetRangeResult;
import com.aliyun.openservices.ots.model.PrimaryKeyType;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;
import com.aliyun.openservices.ots.model.RangeRowQueryCriteria;
import com.aliyun.openservices.ots.model.Row;
import com.aliyun.openservices.ots.model.RowPrimaryKey;
import com.aliyun.openservices.ots.model.TableMeta;

public class GetFirstRowPrimaryKeyCallable implements Callable<RowPrimaryKey>{

    private OTSClient ots = null;
    private TableMeta meta = null;
    private RangeRowQueryCriteria criteria = null;
    
    public GetFirstRowPrimaryKeyCallable(OTSClient ots, TableMeta meta, RangeRowQueryCriteria criteria) {
        this.ots = ots;
        this.meta = meta;
        this.criteria = criteria;
    }
    
    @Override
    public RowPrimaryKey call() throws Exception {
        RowPrimaryKey ret = new RowPrimaryKey();
        GetRangeRequest request = new GetRangeRequest();
        request.setRangeRowQueryCriteria(criteria);
        GetRangeResult result = ots.getRange(request);
        List<Row> rows = result.getRows();
        if(rows.isEmpty()) {
            return null;// no data
        } 
        Row row = rows.get(0);

        Map<String, PrimaryKeyType> pk = meta.getPrimaryKey();
        for (String key:pk.keySet()) {
            ColumnValue v = row.getColumns().get(key);
            if (v.getType() ==  ColumnType.INTEGER) {
                ret.addPrimaryKeyColumn(key, PrimaryKeyValue.fromLong(v.asLong()));
            } else {
                ret.addPrimaryKeyColumn(key, PrimaryKeyValue.fromString(v.asString()));
            }
        }
        return ret;
    }

}
