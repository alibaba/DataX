package com.alibaba.datax.common.element;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

import static com.alibaba.datax.common.exception.DataXException.asDataXException;

/**
 * Created by SunKang on 22-11-3.
 */
public class ArrayColumn extends Column {

	public ArrayColumn(Object[] array) {
		super(array, Type.ARRAY, 1);
	}

	public ArrayColumn() {
		super(null, Type.ARRAY, 1);
	}

	@Override
	public Long asLong() {
		throw DataXException.asDataXException(
				CommonErrorCode.CONVERT_NOT_SUPPORT, String.format(
						"数组类型 [\"%s\"] 不能转为Long.", this.asString()));
	}

	@Override
	public Double asDouble() {
		throw DataXException.asDataXException(
				CommonErrorCode.CONVERT_NOT_SUPPORT, String.format(
						"数组类型 [\"%s\"] 不能转为Double.", this.asString()));
	}

	@Override
	public String asString() {
		return this.toString();
	}

	@Override
	public Date asDate() {
		throw DataXException.asDataXException(
				CommonErrorCode.CONVERT_NOT_SUPPORT, String.format(
						"数组类型 [\"%s\"] 不能转为Date.", this.asString()));
	}

	@Override
	public Date asDate(String dateFormat) {
		throw DataXException.asDataXException(
				CommonErrorCode.CONVERT_NOT_SUPPORT, String.format(
						"数组类型 [\"%s\"] 不能转为Date.", this.asString()));
	}

	@Override
	public byte[] asBytes() {
		throw DataXException.asDataXException(
				CommonErrorCode.CONVERT_NOT_SUPPORT, String.format(
						"数组类型 [\"%s\"] 不能转为Bytes.", this.asString()));
	}

	@Override
	public Boolean asBoolean() {
		throw DataXException.asDataXException(
				CommonErrorCode.CONVERT_NOT_SUPPORT, String.format(
						"数组类型 [\"%s\"] 不能转为Boolean.", this.asString()));
	}

	@Override
	public BigDecimal asBigDecimal() {
		throw DataXException.asDataXException(
				CommonErrorCode.CONVERT_NOT_SUPPORT, String.format(
						"数组类型 [\"%s\"] 不能转为BigDecimal.", this.asString()));
	}

	@Override
	public BigInteger asBigInteger() {
		throw DataXException.asDataXException(
				CommonErrorCode.CONVERT_NOT_SUPPORT, String.format(
						"数组类型 [\"%s\"] 不能转为BigInteger.", this.asString()));
	}
}
