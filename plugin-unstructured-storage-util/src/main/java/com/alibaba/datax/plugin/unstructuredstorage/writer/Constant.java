package com.alibaba.datax.plugin.unstructuredstorage.writer;

public class Constant {

	public static final String DEFAULT_ENCODING = "UTF-8";

	public static final char DEFAULT_FIELD_DELIMITER = ',';

	public static final String DEFAULT_NULL_FORMAT = "\\N";
	
	public static final String FILE_FORMAT_CSV = "csv";
	
	public static final String FILE_FORMAT_TEXT = "text";
	
	//每个分块10MB，最大10000个分块
	public static final Long MAX_FILE_SIZE = 1024 * 1024 * 10 * 10000L;
	
	public static final String DEFAULT_SUFFIX = "";
}
