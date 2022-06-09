package com.alibaba.datax.plugin.unstructuredstorage.util;


public class HdfsUtil {
    private static final double SCALE_TWO = 2.0;
    private static final double SCALE_TEN = 10.0;
    private static final int BIT_SIZE = 8;
    public static int computeMinBytesForPrecision(int precision){

        int numBytes = 1;
        while (Math.pow(SCALE_TWO, BIT_SIZE * numBytes - 1.0) < Math.pow(SCALE_TEN, precision)) {
            numBytes += 1;
        }
        return numBytes;
    }
}
