package com.alibaba.datax.transformer.maskingMethods.anonymity;

/**
 * Created by Liu Kun on 2018/5/8.
 *
 * origin 整形数据
 * 保持原始数据大小顺序，但数值上发生变化。
 */
public class EnumerateMasker {
    public static long mask(long origin, long offset)throws Exception{
        // offset为0则保持原数据不变
        if(offset == 0){
            return origin;
        }
        double expand = (offset * offset * offset * 168) % 1024 ;
        return (int)(expand * (origin + offset));
    }
}
