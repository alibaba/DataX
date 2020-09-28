package com.alibaba.datax.transformer;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.transformer.maskingMethods.differentialPrivacy.EpsilonDifferentialPrivacyImpl;
import com.alibaba.datax.transformer.maskingMethods.irreversibleInterference.MD5EncryptionImpl;

import java.util.Arrays;

/**
 * Created by Liu Kun on 2018/5/9.
 */
public class EDPTransformer extends Transformer{
    private Object masker;
    String key;
    int columnIndex;

    public EDPTransformer(){
        setTransformerName("dx_edp");
        System.out.println("Using Epsilon Differential Privacy masker");
    }

    @Override
    public Record evaluate(Record record, Object... paras) {
        try {
            if (paras.length < 2) {
                throw new RuntimeException("dx_edp transformer缺少参数");
            }
            columnIndex = (Integer) paras[0];
            key = (String) paras[1];
        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
        }
        Column column = record.getColumn(columnIndex);
        try {
            String oriValue = column.asString();
            if (oriValue == null) {
                return record;
            }
            if(column.getType() == Column.Type.DOUBLE) {
                double newValue;
                EpsilonDifferentialPrivacyImpl masker = new EpsilonDifferentialPrivacyImpl();
                newValue = masker.maskOne(Double.parseDouble(oriValue), Double.parseDouble(key));
                record.setColumn(columnIndex, new DoubleColumn(newValue));
            }
            else if(column.getType() == Column.Type.LONG){
                long newValue;
                EpsilonDifferentialPrivacyImpl masker = new EpsilonDifferentialPrivacyImpl();
                newValue = masker.maskOne(Long.valueOf(oriValue), Double.parseDouble(key));
                record.setColumn(columnIndex, new LongColumn(newValue));
            }
        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(), e);
        }
        return record;
    }
}
