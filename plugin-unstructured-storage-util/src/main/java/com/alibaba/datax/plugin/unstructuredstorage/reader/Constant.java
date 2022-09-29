package com.alibaba.datax.plugin.unstructuredstorage.reader;

public class Constant {
	public static final String DEFAULT_ENCODING = "UTF-8";

	public static final char DEFAULT_FIELD_DELIMITER = ',';

	public static final boolean DEFAULT_SKIP_HEADER = false;

	public static final String DEFAULT_NULL_FORMAT = "\\N";
	
    public static final Integer DEFAULT_BUFFER_SIZE = 8192;

	public static final String FILE_FORMAT_CSV = "csv";

	public static final String FILE_FORMAT_TEXT = "text";

	public static final String FILE_FORMAT_EXCEL = "excel";

	public static final String FILE_FORMAT_BINARY = "binary";

	public static final String DEFAULT_FILE_FORMAT = "csv";

	public static final Boolean DEFAULE_SKIP_TEXT_EMPTY_RECORDS = true;

	public static final String EXCEL_VERSION_03_OR_EARLIER = "03_OR_EARLIER";

	public static final String EXCEL_VERSION_07_OR_LATER = "07_OR_LATER";

	/**
	 * 文件全限定名
	 * */
	public static final String SOURCE_FILE = "sourceFile";

	/**
	 * 单纯的文件名
	 * */
	public static final String SOURCE_FILE_NAME = "sourceFileName";

	public static final boolean DEFAULT_OUTPUT_SHEET_NAME = false;

	/**
	 * TODO 暂时先不考虑整个文件夹同步
	 * 在同步音视频等二进制文件的情况下:
	 * 半结构读插件(txtfilreader, ftpreader, hdfsreader, ossreader)需要将相对文件路径注入 RELATIVE_SOURCE_FILE 属性
	 * 目的是半结构化写插件可以统一使用 RELATIVE_SOURCE_FILE 获取到读端插件的所有二进制文件名及其相对路径。
	 * 举个栗子:
	 * 读端插件PATH配置了/home/admin/myapp/
	 */
	public static final String RELATIVE_SOURCE_FILE = "relativeSourceFile";

	/**
	 * 默认读取二进制文件一次性读取的Byte数目: 1048576 Byte [1MB]
	 */
	public static final int DEFAULT_BLOCK_SIZE_IN_BYTE = 1048576;
}
