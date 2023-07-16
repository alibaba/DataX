package com.alibaba.datax.plugin.reader.txtfilereader;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * Created by haiwei.luo on 14-9-20.
 */
public enum TxtFileReaderErrorCode implements ErrorCode {
	REQUIRED_VALUE("TxtFileReader-00", "您缺失了必须填写的参数值."),
	ILLEGAL_VALUE("TxtFileReader-01", "您填写的参数值不合法."),
	MIXED_INDEX_VALUE("TxtFileReader-02", "您的列信息配置同时包含了index,value."),
	NO_INDEX_VALUE("TxtFileReader-03","您明确的配置列信息,但未填写相应的index,value."),
	FILE_NOT_EXISTS("TxtFileReader-04", "您配置的目录文件路径不存在."),
	OPEN_FILE_WITH_CHARSET_ERROR("TxtFileReader-05", "您配置的文件编码和实际文件编码不符合."),
	OPEN_FILE_ERROR("TxtFileReader-06", "您配置的文件在打开时异常,建议您检查源目录是否有隐藏文件,管道文件等特殊文件."),
	READ_FILE_IO_ERROR("TxtFileReader-07", "您配置的文件在读取时出现IO异常."),
	SECURITY_NOT_ENOUGH("TxtFileReader-08", "您缺少权限执行相应的文件操作."),
	CONFIG_INVALID_EXCEPTION("TxtFileReader-09", "您的参数配置错误."),
	RUNTIME_EXCEPTION("TxtFileReader-10", "出现运行时异常, 请联系我们"),
	EMPTY_DIR_EXCEPTION("TxtFileReader-11", "您尝试读取的文件目录为空."),;

	private final String code;
	private final String description;

	private TxtFileReaderErrorCode(String code, String description) {
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
