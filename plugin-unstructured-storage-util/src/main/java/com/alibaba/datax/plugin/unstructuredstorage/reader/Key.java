package com.alibaba.datax.plugin.unstructuredstorage.reader;

/**
 * Created by haiwei.luo on 14-12-5.
 */
public class Key {
	public static final String COLUMN = "column";

	public static final String ENCODING = "encoding";

	public static final String FIELD_DELIMITER = "fieldDelimiter";

	public static final String SKIP_HEADER = "skipHeader";

	public static final String TYPE = "type";

	public static final String FORMAT = "format";

	public static final String INDEX = "index";

	public static final String VALUE = "value";

	public static final String COMPRESS = "compress";

	public static final String NULL_FORMAT = "nullFormat";

	public static final String BUFFER_SIZE = "bufferSize";

	public static final String CSV_READER_CONFIG = "csvReaderConfig";

	public static final String MARK_DONE_FILE_NAME = "markDoneFileName";

	public static final String MARK_DOING_FILE_NAME = "markDoingFileName";

	// public static final String RETRY_TIME = "retryTime";
	public final static String MAX_RETRY_TIME = "maxRetryTime";

	public final static String RETRY_INTERVAL = "retryInterval";

	public static final String TEXT_READER_CONFIG = "textReaderConfig";

	public static final String SKIP_EMPTY_RECORDS = "skipEmptyRecords";

	public static final String EXCEL_READER_CONFIG = "excelReaderConfig";

	public static final String EXCEL_SHEET_NAME = "excelSheetName";

	public static final String VERSION = "version";

	public static final String OUTPUT_SHEET_NAME = "outputSheetName";

	/**
	 * csv or text or excel
	 */
	public static final String FILE_FORMAT = "fileFormat";

	/**
	 * 是否把一个file当做一个column
	 */
	public static final String FILE_AS_COLUMN = "fileAsColumn";

	/**
	 * 读取二进制文件一次性读取的Byte数目
	 */
	public static final String BLOCK_SIZE_IN_BYTE = "blockSizeInByte";

	/**
	 * 半结构化标示一个Record来源的绝对文件路径名，可以是ftp文件，oss的object等
	 * */
	public static final String META_KEY_FILE_PATH = "filePath";

	/**
	 * 多文件切分的工作项，Task通过此配置项表示工作内容, 文件内部切分相关key
	 */
	public static final String SPLIT_SLICE_CONFIG = "__splitSliceConfig";
	public static final String SPLIT_SLICE_FILE_PATH = "filePath";
	public static final String SPLIT_SLICE_START_POINT = "startPoint";
	public static final String SPLIT_SLICE_END_POINT = "endPoint";

	/**
	 * tar.gz压缩包，支持配置 tarFileFilterPattern 参数，来过滤要同步的文件
	 * For Example:
	 * "tarFileFilterPattern" ： "*.dat"
	 *
	 * 同步的时候，只同步 tar.gz 里面文件名后缀为 .dat 的文件
	 */
	public static final String TAR_FILE_FILTER_PATTERN = "tarFileFilterPattern";
	public static final String ENABLE_INNER_SPLIT = "enableInnerSplit";

}
