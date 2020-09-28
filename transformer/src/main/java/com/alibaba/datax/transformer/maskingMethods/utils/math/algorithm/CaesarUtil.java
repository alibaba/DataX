package com.alibaba.datax.transformer.maskingMethods.utils.math.algorithm;

import java.util.LinkedList;
import java.util.List;

/**
 * 可逆置换之凯撒算法脱敏
 * Caesar Substitution
 * created by zouyuanyuan on 2018/01/09
 */
public class CaesarUtil {

    public static List<String> caesarCipher(List<String> col, int k) {
        List<String> resCol = new LinkedList<String>();
        try {
            int colLen = col.size();
            for (int i = 0; i < colLen; i++) {
                resCol.add(col.get((k + i) % colLen));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            return resCol;
        }
    }

    public static List<String> caesarCipherDecrypt(List<String> col, int k) {
        int colLen = col.size();
        List<String> resCol = new LinkedList<String>();
        for (int i = 0; i < colLen; i++) {
            int j = (i - k) % colLen;
            while (j < 0) j += colLen;
            resCol.add(col.get(j));
        }
        return resCol;
    }

}
