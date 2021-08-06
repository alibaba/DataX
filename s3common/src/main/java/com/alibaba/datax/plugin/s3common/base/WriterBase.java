package com.alibaba.datax.plugin.s3common.base;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.plugin.RecordReceiver;

import java.io.IOException;

/**
 * Author: duhanmin
 * Description:
 * Date: 2021/6/10 10:05
 */
public interface WriterBase<T> {

    /**
     * 初始化方法
     * 例如数据源连接,文件流打开等
     */
    void init();

    /**
     * 类型转换
     * @param compression
     * @return
     * @throws Exception
     */
    Object compressType(String compression) throws Exception;

    /**
     * 生成Schema
     */
    void buildSchemas();

    /**
     * 写数据
     * @param lineReceiver
     * @throws IOException
     */
    void writer(RecordReceiver lineReceiver) throws IOException;

    /**
     * 数据转换
     * @param record
     * @param column
     * @param i
     */
    void addData(T record, Column column, int i);

    /**
     * 资源关闭方法
     * @throws IOException
     */
    void close() throws IOException;

}
