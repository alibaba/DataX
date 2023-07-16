package com.alibaba.datax.plugin.reader.hbase11xreader.util;

import org.apache.commons.lang.StringUtils;

public class MessyCodeCheck {


	/**
     * 判断字符是否是中文
     *
     * @param c 字符
     * @return 是否是中文
     */
    public static boolean isChinese(char c) {
        Character.UnicodeBlock ub = Character.UnicodeBlock.of(c);
        if (ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS
                || ub == Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS
                || ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A
                || ub == Character.UnicodeBlock.GENERAL_PUNCTUATION
                || ub == Character.UnicodeBlock.CJK_SYMBOLS_AND_PUNCTUATION
                || ub == Character.UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS) {
            return true;
        }
        return false;
    }
 
    /**
     * 
     * @Title: isMessyCode   
     * @Description: 包含"�",那么是乱码
     * @param: @param strName
     * @param: @return      
     * @return: boolean      
     * @throws
     */
    public static boolean isMessyCode(String strName) {
    	if(StringUtils.isBlank(strName)) {
    		return false;
    	}
        return strName.contains("�");
 
    }
 
    public static void main(String[] args) {
        System.out.println(isMessyCode("Ã©Å¸Â©Ã©Â¡ÂºÃ¥Â¹Â³"));
        System.out.println(isMessyCode("你好"));
        System.out.println(isMessyCode("f1e�d�]�<`k\\u00076���\\t:�aE�\\u0002��?��m�~��\\u0004儫\\u000fP"));
    }
	
}
