package com.alibaba.datax.transformer.maskingMethods.utils.math.algorithm;

import java.util.LinkedList;
import java.util.List;

/**
 * 可逆置换之栅栏置换（还是置换，不改变属性真实值，只是将属性值的位置变换）
 * 栅栏密码
 * 基本思想：多栏栅栏移位*把属性列中的记录分成k个一组，然后把每组的相应位置的记录连起来，形成脱敏的结果。
 * 实现手段：手工
 * 算法特征：
 * 输入：单条属性列col，固定数目k
 * 输出：偏移的属性列col
 */
public class RailFenceUtil {

    public static List<String> railFenceCipher(List<String> col, int k) {
        List<String> resCol = new LinkedList<String>();
        try {
            int colLen = col.size();
            for (int i = 0; i < k; i ++) {
                for (int j = 0; j <colLen ; j += k) {
                    int loc = i + j;
                    if (loc < colLen) {
                        resCol.add(col.get(loc));
                    }
                }
            }
        } catch (RuntimeException e) {
            e.printStackTrace();
        } finally {
            return resCol;
        }
    }

}
