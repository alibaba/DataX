package com.alibaba.datax.plugin.writer.osswriter;

/**
 * Created by haiwei.luo on 15-02-09.
 */
public class Constant {
    public static final String OBJECT = "object";
    public static final int SOCKETTIMEOUT = 5000000;
    public static final String DEFAULT_NULL_FORMAT = "null";

    /**
     * 每一个上传的Part都有一个标识它的号码（part number，范围是1-10000）
     * https://help.aliyun.com/document_detail/31993.html
     */
    public static final int MAX_BLOCK_SIZE = 10000;
}
