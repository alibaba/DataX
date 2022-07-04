package com.alibaba.datax.plugin.unstructuredstorage.writer.binaryFileUtil;

import com.alibaba.datax.common.spi.ErrorCode;

public enum BinaryFileWriterErrorCode implements ErrorCode {
	ILLEGAL_VALUE("UnstructuredStorageWriter-00", "errorcode.illegal_value"),
	REPEATED_FILE_NAME("UnstructuredStorageWriter-01", "errorcode.repeated_file_name"),
	REQUIRED_VALUE("UnstructuredStorageWriter-02","errorcode.required_value"),;

	private final String code;
	private final String description;

	private BinaryFileWriterErrorCode(String code, String description) {
		this.code = code;
		this.description = description;
	}

	@Override
	public String getCode() {
		return this.code;
	}

	@Override
	public String getDescription() {
		return this.description;
	}

	@Override
	public String toString() {
		return String.format("Code:[%s], Description:[%s].", this.code,
				this.description);
	}
}
