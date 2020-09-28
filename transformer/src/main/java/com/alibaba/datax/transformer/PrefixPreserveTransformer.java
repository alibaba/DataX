package com.alibaba.datax.transformer;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.transformer.maskingMethods.anonymity.FloorMasker;
import com.alibaba.datax.transformer.maskingMethods.anonymity.PrefixPreserveMasker;

import java.util.Arrays;
import java.util.Date;

/**
 * Created by Liu Kun on 2018/5/9.
 */
public class PrefixPreserveTransformer extends Transformer{
    private Object masker;
    String key;
    int columnIndex;

    public PrefixPreserveTransformer(){
        setTransformerName("dx_prefix_preserve");
        System.out.println("Using prefix preserve masker");
    }

    @Override
    public Record evaluate(Record record, Object... paras) {
        try {
            if (paras.length < 2) {
                throw new RuntimeException("dx_prefix_preserve transformer缺少参数");
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
            if(column.getType() == Column.Type.STRING){
                String newValue = PrefixPreserveMasker.mask(column.asString(), Integer.valueOf(key));
                record.setColumn(columnIndex, new StringColumn(newValue));
            }
            else if(column.getType() == Column.Type.LONG){
                Long newValue = PrefixPreserveMasker.mask(column.asLong(), Integer.valueOf(key));
                record.setColumn(columnIndex, new LongColumn(newValue));
            }
        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(), e);
        }
        return record;
    }
}
