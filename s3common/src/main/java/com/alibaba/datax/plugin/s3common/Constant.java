package com.alibaba.datax.plugin.s3common;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * s3 props
 *
 * Author: duhanmin
 * Description:
 * Date: 2021/6/8 10:18
 */
public interface Constant {

    String ACCESSKEY = "accessKey";

    String SECRETKEY = "secretKey";

    String BUCKET = "bucket";

    String PATH = "path";

    String STORED = "stored";

    String WRITE_MODE = "writeMode";

    String COMPRESSION = "compression";

    String FIELD_DELIMITER_KEY = "fieldDelimiter";

    String COLUMN = "column";

    String NAME = "name";

    String TYPE = "type";

    String INDEX = "index";

    String SOURCE_FILES = "sourceFiles";

    Charset CHARSET = StandardCharsets.UTF_8;

    Integer DEFAULT_BUFFER_SIZE = 8192;

    String FIELD_DELIMITER = "\u0001";
}
