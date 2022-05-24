package com.alibaba.datax.plugin.s3common.util;

import org.apache.parquet.io.api.Binary;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Author: duhanmin
 * Description:
 * Date: 2021/7/8 20:00
 */
public class DecimalUtil {

    public static BigDecimal binaryToDecimal(Binary binary, int scale){
        BigInteger bi = new BigInteger(binary.getBytes());
        return new BigDecimal(bi, scale);
    }

    public static BigDecimal longToDecimal(long value, int scale){
        BigInteger bi = BigInteger.valueOf(value);
        return new BigDecimal(bi, scale);
    }
}
