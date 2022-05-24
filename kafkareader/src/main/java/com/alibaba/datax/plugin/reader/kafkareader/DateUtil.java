package com.alibaba.datax.plugin.reader.kafkareader;

import javafx.scene.input.DataFormat;
import org.apache.commons.lang3.StringUtils;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public final class DateUtil {
    public static final String FP = "yyyyMMdd";
    public static final String FM = "yyyyMM";
    public static final String FPT = "yyyyMMddHHmmss";
    public static final String FW = "yyyy-MM-dd";
    public static final String MW = "yyyy-MM";
    public static final String FL = "yyyy-MM-dd HH:mm:ss";
    public static final String FDOTMD = "M.d";
    public static final String FCHN = "yyyy年M月d日";
    public static final String FCHNYM = "yyyy年M月";
    public static final String FCHNMD = "M月d日";
    public static final String FCHNM = "M月";

    public DateUtil() {
    }


    public static final DateFormat newDateFormat(String fmt) {
        return new SimpleDateFormat(fmt);
    }


    public static String targetFormat(Date date){
        return date == null ? "" : newDateFormat("yyyy-MM-dd HH:mm:ss").format(date);

    }

    public static String targetFormat(Date date, String format){
        return date == null ? "" : newDateFormat(format).format(date);
    }


    public static void main(String[] args){

//        String ss ;
//        ss=targetFormat(new Date());
//
//        String bb = ss.split(" ")[1].substring(0, 2);
//        Boolean aa = ss.split(" ")[1].substring(0, 2).equals("00");
//        //判断当前事件是不是0点,0点的话进程他退出
////        Date date = new Date();
////        if (DateUtil.targetFormat(date).split(" ")[1].substring(0, 2).equals("00")) {
////        }

        String[] columns = "a,b,c,d".split(",");
        System.out.println(columns);
    }



}
