package com.alibaba.datax.common.element;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.fastjson.JSON;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Array;
import java.sql.SQLException;
import java.util.Date;

public class ArrayColumn
        extends Column {
    public ArrayColumn(Object rawData) throws SQLException {
        // TODO 这里需要实现度量 array 类型的 rawData  的size的方法--临时写成这样
        super(rawData, Type.ARRAY, (rawData == null) ? 0 : JSON.toJSONString(((Array) rawData).getArray()).length());
    }

    @Override
    public Long asLong() {
        throw DataXException.asDataXException(
                CommonErrorCode.CONVERT_NOT_SUPPORT, String.format(
                        "String [\"%s\"] 不能转为Long.", this.asString()));
    }

    @Override
    public Double asDouble() {
        throw DataXException.asDataXException(
                CommonErrorCode.CONVERT_NOT_SUPPORT, String.format(
                        "String [\"%s\"] 不能转为Double.", this.asString()));
    }

    @Override
    public String asString() {
        return this.toString();
    }

    @Override
    public Date asDate() {
        throw DataXException.asDataXException(
                CommonErrorCode.CONVERT_NOT_SUPPORT, String.format(
                        "String [\"%s\"] 不能转为Date.", this.asString()));
    }

    @Override
    public byte[] asBytes() {
        throw DataXException.asDataXException(
                CommonErrorCode.CONVERT_NOT_SUPPORT, String.format(
                        "String [\"%s\"] 不能转为byte[].", this.asString()));
    }

    @Override
    public Boolean asBoolean() {
        throw DataXException.asDataXException(
                CommonErrorCode.CONVERT_NOT_SUPPORT, String.format(
                        "String [\"%s\"] 不能转为Boolean.", this.asString()));
    }

    @Override
    public BigDecimal asBigDecimal() {
        throw DataXException.asDataXException(
                CommonErrorCode.CONVERT_NOT_SUPPORT, String.format(
                        "String [\"%s\"] 不能转为BigDecimal.", this.asString()));
    }

    @Override
    public BigInteger asBigInteger() {
        throw DataXException.asDataXException(
                CommonErrorCode.CONVERT_NOT_SUPPORT, String.format(
                        "String [\"%s\"] 不能转为BigInteger.", this.asString()));
    }

    @Override
    public Array asArray() {
        return (Array) getRawData();
    }
}
