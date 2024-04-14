package com.alibaba.datax.core.transport.transformer;

import org.apache.commons.codec.digest.DigestUtils;

/**
 * GroovyTransformer的帮助类，供groovy代码使用，必须全是static的方法
 * Created by liqiang on 16/3/4.
 */
public class GroovyTransformerStaticUtil  {

    public static String md5(final String data) {
        return DigestUtils.md5Hex(data);
    }

    public static String sha1(final String data) {
        return DigestUtils.sha1Hex(data);
    }
}
