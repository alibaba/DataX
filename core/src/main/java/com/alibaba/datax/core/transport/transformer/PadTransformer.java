package com.alibaba.datax.core.transport.transformer;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.transformer.Transformer;

import java.util.Arrays;

/**
 * no comments.
 * Created by liqiang on 16/3/4.
 */
public class PadTransformer extends Transformer {
    public PadTransformer() {
        setTransformerName("dx_pad");
    }

    @Override
    public Record evaluate(Record record, Object... paras) {

        int columnIndex;
        String padType;
        int length;
        String padString;

        try {
            if (paras.length != 4) {
                throw new RuntimeException("dx_pad paras must be 4");
            }

            columnIndex = (Integer) paras[0];
            padType = (String) paras[1];
            length = Integer.valueOf((String) paras[2]);
            padString = (String) paras[3];
        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
        }

        Column column = record.getColumn(columnIndex);

        try {
            String oriValue = column.asString();

            //如果字段为空，作为空字符串处理
            if(oriValue==null){
                oriValue = "";
            }
            String newValue;
            if (!padType.equalsIgnoreCase("r") && !padType.equalsIgnoreCase("l")) {
                throw new RuntimeException(String.format("dx_pad first para(%s) support l or r", padType));
            }
            if (length <= oriValue.length()) {
                newValue = oriValue.substring(0, length);
            } else {

                newValue = doPad(padType, oriValue, length, padString);
            }

            record.setColumn(columnIndex, new StringColumn(newValue));

        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(),e);
        }
        return record;
    }

    private String doPad(String padType, String oriValue, int length, String padString) {

        String finalPad = "";
        int NeedLength = length - oriValue.length();
        while (NeedLength > 0) {

            if (NeedLength >= padString.length()) {
                finalPad += padString;
                NeedLength -= padString.length();
            } else {
                finalPad += padString.substring(0, NeedLength);
                NeedLength = 0;
            }
        }

        if (padType.equalsIgnoreCase("l")) {
            return finalPad + oriValue;
        } else {
            return oriValue + finalPad;
        }
    }

}
