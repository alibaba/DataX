package com.alibaba.datax.plugin.writer.db2writer;

import com.alibaba.datax.common.spi.ErrorCode;

public enum DB2WriterErrorCode implements ErrorCode {
	;
	private final String code;
	private final String describe;

	private DB2WriterErrorCode(String code, String describe) {
		this.code = code;
		this.describe = describe;
	}

	public String getCode() {
		return this.code;
	}

	public String getDescription() {
		return this.describe;
	}

	public String toString() {
		return String.format("Code:[%s], Describe:[%s]. ", new Object[] { this.code, this.describe });
	}
}
