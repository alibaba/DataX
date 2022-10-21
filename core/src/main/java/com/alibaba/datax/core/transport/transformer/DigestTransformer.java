package com.alibaba.datax.core.transport.transformer;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.transformer.Transformer;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;

/**
 * no comments.
 *
 * @author XuDaojie
 * @since 2021-08-16
 */
public class DigestTransformer extends Transformer {

    private static final String MD5 = "md5";
    private static final String SHA1 = "sha1";
    private static final String TO_UPPER_CASE = "toUpperCase";
    private static final String TO_LOWER_CASE = "toLowerCase";

    public DigestTransformer() {
        setTransformerName("dx_digest");
    }

    @Override
    public Record evaluate(Record record, Object... paras) {

        int columnIndex;
        String type;
        String charType;

        try {
            if (paras.length != 3) {
                throw new RuntimeException("dx_digest paras length must be 3");
            }

            columnIndex = (Integer) paras[0];
            type = (String) paras[1];
            charType = (String) paras[2];

            if (!StringUtils.equalsIgnoreCase(MD5, type) && !StringUtils.equalsIgnoreCase(SHA1, type)) {
                throw new RuntimeException("dx_digest paras index 1 must be md5 or sha1");
            }
            if (!StringUtils.equalsIgnoreCase(TO_UPPER_CASE, charType) && !StringUtils.equalsIgnoreCase(TO_LOWER_CASE, charType)) {
                throw new RuntimeException("dx_digest paras index 2 must be toUpperCase or toLowerCase");
            }
        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras) + " => " + e.getMessage());
        }

        Column column = record.getColumn(columnIndex);

        try {
            String oriValue = column.asString();

            // 如果字段为空，作为空字符串处理
            if (oriValue == null) {
                oriValue = "";
            }
            String newValue;
            if (MD5.equals(type)) {
                newValue = DigestUtils.md5Hex(oriValue);
            } else {
                newValue = DigestUtils.sha1Hex(oriValue);
            }

            if (TO_UPPER_CASE.equals(charType)) {
                newValue = newValue.toUpperCase();
            } else {
                newValue = newValue.toLowerCase();
            }

            record.setColumn(columnIndex, new StringColumn(newValue));

        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(), e);
        }
        return record;
    }

}
