package com.alibaba.datax.common.element;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import org.apache.commons.lang3.ArrayUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

/**
 * Created by jingxing on 14-8-24.
 */
public class BytesColumn extends Column {

	public BytesColumn() {
		this(null);
	}

	public BytesColumn(byte[] bytes) {
		super(ArrayUtils.clone(bytes), Column.Type.BYTES, null == bytes ? 0
				: bytes.length);
	}

	@Override
	public byte[] asBytes() {
		if (null == this.getRawData()) {
			return null;
		}

		return (byte[]) this.getRawData();
	}

	@Override
	public String asString() {
		if (null == this.getRawData()) {
			return null;
		}

		try {
			return ColumnCast.bytes2String(this);
		} catch (Exception e) {
			throw DataXException.asDataXException(
					CommonErrorCode.CONVERT_NOT_SUPPORT,
					String.format("Bytes[%s]不能转为String .", this.toString()));
		}
	}

	@Override
	public Long asLong() {
		throw DataXException.asDataXException(
				CommonErrorCode.CONVERT_NOT_SUPPORT, "Bytes类型不能转为Long .");
	}

	@Override
	public BigDecimal asBigDecimal() {
		throw DataXException.asDataXException(
				CommonErrorCode.CONVERT_NOT_SUPPORT, "Bytes类型不能转为BigDecimal .");
	}

	@Override
	public BigInteger asBigInteger() {
		throw DataXException.asDataXException(
				CommonErrorCode.CONVERT_NOT_SUPPORT, "Bytes类型不能转为BigInteger .");
	}

	@Override
	public Double asDouble() {
		throw DataXException.asDataXException(
				CommonErrorCode.CONVERT_NOT_SUPPORT, "Bytes类型不能转为Long .");
	}

	@Override
	public Date asDate() {
		throw DataXException.asDataXException(
				CommonErrorCode.CONVERT_NOT_SUPPORT, "Bytes类型不能转为Date .");
	}
	
	@Override
	public Date asDate(String dateFormat) {
		throw DataXException.asDataXException(
				CommonErrorCode.CONVERT_NOT_SUPPORT, "Bytes类型不能转为Date .");
	}

	@Override
	public Boolean asBoolean() {
		throw DataXException.asDataXException(
				CommonErrorCode.CONVERT_NOT_SUPPORT, "Bytes类型不能转为Boolean .");
	}
}
