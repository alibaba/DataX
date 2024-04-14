package com.alibaba.datax.plugin.unstructuredstorage.writer;

public class Constant {

	public static final String DEFAULT_ENCODING = "UTF-8";

	public static final char DEFAULT_FIELD_DELIMITER = ',';

	public static final String DEFAULT_NULL_FORMAT = "\\N";
	
	public static final String FILE_FORMAT_CSV = "csv";
	
	public static final String FILE_FORMAT_TEXT = "text";

	public static final String FILE_FORMAT_SQL = "sql";

	//每个分块10MB，最大10000个分块, MAX_FILE_SIZE 单位: MB
	public static final Long MAX_FILE_SIZE = 10 * 10000L;

	public static final int DEFAULT_COMMIT_SIZE = 2000;

	public static final String DEFAULT_SUFFIX = "";

	public static final String TRUNCATE = "truncate";
	public static final String APPEND = "append";
	public static final String NOCONFLICT = "nonConflict";

	/**
	 * 在同步音视频等二进制文件的情况下:
	 * 半结构化写插件可以统一使用 SOURCE_FILE 获取到读端插件的split file路径
	 */
	public static final String SOURCE_FILE = "sourceFile";

	public static final String SOURCE_FILE_NAME = "sourceFileName";

	/**
	 * 是否是音视频等无结构化文件
	 */
	public static final String BINARY = "binary";

	/**
	 * 文件同步模式， 如果是copy表示纯文件拷贝
	 * */
	public static final String SYNC_MODE_VALUE_COPY = "copy";
}
