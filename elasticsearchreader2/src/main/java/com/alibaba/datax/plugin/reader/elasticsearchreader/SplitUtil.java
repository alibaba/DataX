package com.alibaba.datax.plugin.reader.elasticsearchreader;


import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;

public class SplitUtil {
    public static int[] dointSplit(int left, int right, int expectSliceNumber) {
        if (expectSliceNumber < 1) {
            throw new IllegalArgumentException(String.format(
                    "切分份数不能小于1. 此处:expectSliceNumber=[%s].", expectSliceNumber));
        }
        if (left == right) {
            return new int[]{left, right};
        }

        // 调整大小顺序，确保 left < right
        if (left - right > 0) {
            int temp = left;
            left = right;
            right = temp;
        }
        int step = (right - left) / expectSliceNumber;


        int[] result = new int[expectSliceNumber + 1];
        result[0] = left;
        result[expectSliceNumber] = right+1;

        int lowerBound;
        int upperBound = left;
        for (int i = 1; i < expectSliceNumber; i++) {
            lowerBound = upperBound;
            upperBound = lowerBound+step;
            result[i] = upperBound+1;
        }

        return result;
    }

    public static void main(String[] args) {
        int left=0;
        int right=63;
        int channel=10;
        int[] split = dointSplit(left, right, channel);
        System.out.println(Arrays.toString(split));
        for (int i = 0; i < split.length-1; i++) {
            System.out.println(split[i]+"  "+split[i+1]);
        }
    }

}
