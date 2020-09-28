package com.alibaba.datax.transformer;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.transformer.maskingMethods.anonymity.EnumerateMasker;
import com.alibaba.datax.transformer.maskingMethods.anonymity.FloorMasker;

import java.util.Arrays;
import java.util.Date;

/**
 * Created by Liu Kun on 2018/5/9.
 */
public class EnumerateTransformer extends Transformer{
    private Object masker;
    String key;
    int columnIndex;

    public EnumerateTransformer() {
        setTransformerName("dx_enum");
        System.out.println("Using enumerate masker");
    }

    @Override
    public Record evaluate(Record record, Object... paras) {
        try {
            if (paras.length < 2) {
                throw new RuntimeException("dx_enum transformer缺少参数");
            }
            columnIndex = (Integer) paras[0];
            key = String.valueOf(paras[1]);
        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
        }
        Column column = record.getColumn(columnIndex);
        try {
            String oriValue = column.asString();
            if(oriValue == null){
                return  record;
            }
            int offset = Integer.parseInt(key);
            double newValue = EnumerateMasker.mask(column.asLong(), offset);
            record.setColumn(columnIndex, new DoubleColumn(newValue));
        } catch (Exception e){
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(),e);
        }
        return record;
    }
}
