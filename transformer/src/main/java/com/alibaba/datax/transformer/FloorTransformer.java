package com.alibaba.datax.transformer;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.transformer.maskingMethods.anonymity.FloorMasker;

import java.util.Arrays;
import java.util.Date;

/**
 * Created by Liu Kun on 2018/5/9.
 */
public class FloorTransformer extends Transformer {
    private Object masker;
    String key;
    int columnIndex;

    public FloorTransformer() {
        setTransformerName("dx_floor");
        System.out.println("Using floor masker");
    }

    @Override
    public Record evaluate(Record record, Object... paras) {
        try {
            if (paras.length < 2) {
                throw new RuntimeException("Floor transformer缺少参数");
            }
            columnIndex = (Integer) paras[0];
            key = String.valueOf(paras[1]);
        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
        }
        Column column = record.getColumn(columnIndex);
        try {
            String oriValue = column.asString();
            if (oriValue == null) {
                return record;
            }
            if (column.getType() == Column.Type.DATE) {
                Date newValue = FloorMasker.mask(column.asDate(), key);
                record.setColumn(columnIndex, new DateColumn(newValue));
            } else if (column.getType() == Column.Type.LONG || column.getType() == Column.Type.INT) {
                int mod = 0;
                if (key.isEmpty()){
                    mod = 10;
                }
                else{
                    mod = Integer.valueOf(key);
                }
                long newValue = FloorMasker.mask(column.asLong(), mod);
                record.setColumn(columnIndex, new LongColumn(newValue));
            } else if (column.getType() == Column.Type.DOUBLE) {
                Double newValue = FloorMasker.mask(column.asDouble());
                record.setColumn(columnIndex, new DoubleColumn(newValue));
            }
        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(), e);
        }
        return record;
    }
}
