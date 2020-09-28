package com.alibaba.datax.transformer.maskingMethods.anonymity;

import java.util.Date;

/**
 * Created by Liu Kun on 2018/5/8.
 */
public interface AnonyMasker {
    String mask(String origin) throws Exception;
    int mask(int origin) throws Exception;
    long mask(long origin) throws Exception;
    double mask(double origin) throws Exception;
    boolean mask(boolean origin) throws Exception;
    Date mask(Date origin) throws Exception;

}
