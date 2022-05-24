package com.alibaba.datax.plugin.s3common.base;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.plugin.RecordSender;

import java.io.IOException;

/**
 * Author: duhanmin
 * Description:
 * Date: 2021/7/7 13:39
 */
public interface ReaderBase<T> {

    /**
     * 读取数据
     * @param sourceFile
     * @param recordSender
     * @throws IOException
     */
    void reader(String sourceFile,RecordSender recordSender);


    /**
     * 构造写入数据
     *
     * @param t
     * @param type
     * @param index
     * @param format
     * @return
     */
    Column addData(T t, String type,int index, String format);

    /**
     * 资源关闭方法
     * @throws IOException
     */
    void close() throws IOException;
}
