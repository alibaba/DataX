package com.alibaba.datax.transformer.maskingMethods.anonymity;

import com.alibaba.datax.transformer.maskingMethods.Masking;
import java.util.Date;

/**
 * Created by Liu Kun on 2018/5/8.
 */

/*
* 将数据替换成一个常量，常用作处理不需要的敏感字段。
* */
public class Hiding implements AnonyMasker{
        public boolean mask(boolean origin) throws Exception{
            return true;
        }
        public Date mask(Date origin)throws Exception{
            Date date = new Date();
            return date;
        }

        public double mask(double origin) throws Exception{
            return 0;
        }

        public int mask(int origin) throws Exception{
            return 0;
        }

        public long mask(long origin)throws Exception{
            return 0;
        }

        public String mask(String origin)throws Exception{
            return "0";
        }
}
