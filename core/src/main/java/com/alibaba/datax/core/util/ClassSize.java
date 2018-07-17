package com.alibaba.datax.core.util;

/**
 * Created by liqiang on 15/12/12.
 */
public class ClassSize {

    public static  final int DefaultRecordHead;
    public static  final int ColumnHead;

    //objectHead的大小
    public static final int REFERENCE;
    public static final int OBJECT;
    public static final int ARRAY;
    public static final int ARRAYLIST;
    static {
        //only 64位
        REFERENCE = 8;

        OBJECT = 2 * REFERENCE;

        ARRAY = align(3 * REFERENCE);

        // 16+8+24+16
        ARRAYLIST = align(OBJECT + align(REFERENCE) + align(ARRAY) +
                (2 * Long.SIZE / Byte.SIZE));
        // 8+64+8
        DefaultRecordHead = align(align(REFERENCE) + ClassSize.ARRAYLIST + 2 * Integer.SIZE / Byte.SIZE);
        //16+4
        ColumnHead = align(2 * REFERENCE + Integer.SIZE / Byte.SIZE);
    }

    public static int align(int num) {
        return (int)(align((long)num));
    }

    public static long align(long num) {
        //The 7 comes from that the alignSize is 8 which is the number of bytes
        //stored and sent together
        return  ((num + 7) >> 3) << 3;
    }
}
