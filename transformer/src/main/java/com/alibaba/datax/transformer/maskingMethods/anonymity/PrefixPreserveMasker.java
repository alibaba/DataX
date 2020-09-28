/*
 * Copyright (c) 2018. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */

package com.alibaba.datax.transformer.maskingMethods.anonymity;

import java.util.ArrayList;

import static java.lang.Math.min;

/**
 * Created by LabUser on 2018/5/8.
 */
public class PrefixPreserveMasker {

    public static char charMap(char origin){
        if(Character.isDigit(origin)){
            origin = (char) ('0' + (int)(Math.random()*10));
        }
        else if(Character.isLetter(origin)){
            int offset = (int)(Math.random()*26);
            origin = origin<=90? (char)('A' + offset):(char)('a' + offset);
        }
        return origin;
    }

    public static long mask(long origin, int preserveNum) throws Exception{
        String ori_str = String.valueOf(origin);
        char[] result = new char[ori_str.length()];
        for(int i=0;i<ori_str.length();i++){
            result[i] = i<preserveNum ? ori_str.charAt(i):charMap(ori_str.charAt(i));
        }
        return Long.valueOf(String.valueOf(result));
    }

    public static String mask(String origin, int preserveNum) throws Exception{
        preserveNum = min(preserveNum, origin.length());
        char[] str = new char[origin.length()+1];
        int i;
        for(i=0;i<preserveNum;i++){
            str[i] = origin.charAt(i);
        }
        for(;i<origin.length();i++){
            str[i] = charMap(origin.charAt(i));
        }
        return String.valueOf(str);
    }
}
