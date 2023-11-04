package com.alibaba.datax.plugin.reader.otsreader.utils;

import com.alicloud.openservices.tablestore.model.PrimaryKeyColumn;

import java.util.List;


public class CompareHelper {
    /**
     * 比较PrimaryKeyColumn List的大小
     * 返回
     * -1 表示before小于after
     *  0 表示before等于after
     *  1 表示before大于after
     *  
     * @param before
     * @param after
     * @return
     */
    public static int comparePrimaryKeyColumnList(List<PrimaryKeyColumn> before, List<PrimaryKeyColumn> after) {
        int size = before.size() < after.size() ? before.size() : after.size();
        
        for (int i = 0; i < size; i++) {
            int cmp = before.get(i).compareTo(after.get(i));
            if (cmp != 0) {
                return cmp;
            }
        }
        
        if (before.size() < after.size() ) {
            return -1;
        } else if (before.size() > after.size() ) {
            return 1;
        }
        return 0;
    }
}
