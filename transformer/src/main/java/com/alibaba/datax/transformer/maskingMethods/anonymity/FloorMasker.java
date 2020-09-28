package com.alibaba.datax.transformer.maskingMethods.anonymity;

import java.util.Calendar;
import java.util.Date;

/**
 * Created by Liu Kun on 2018/5/8.
 */
public class FloorMasker {
    public static long mask(long origin, int mod) throws Exception{
        return origin/mod * mod;
    }

    public static double mask(double origin) throws Exception{
        return (int) origin;
    }

    public static Date mask(Date origin, String level)throws Exception{
        Calendar c = Calendar.getInstance();
        c.setTime(origin);
        if(level.contains("Y")){
            int year = c.get(Calendar.YEAR);
            c.set(Calendar.YEAR, year/10 * 10);
        }
        if(level.contains("M")){
            int month = c.get(Calendar.MONTH);
            c.set(Calendar.MONTH, 0);
        }
        if(level.contains("D")){
            int day = c.get(Calendar.DAY_OF_MONTH);
            c.set(Calendar.DAY_OF_MONTH, day/10 *10 + 1);
        }
        if(level.contains("H")){
            int hour = c.get(Calendar.HOUR);
            c.set(Calendar.HOUR, hour/6 * 6);
        }
        if(level.contains("m")){
            int minute = c.get(Calendar.MINUTE);
            c.set(Calendar.MINUTE, 0);
        }
        if(level.contains("s")){
            int second = c.get(Calendar.SECOND);
            c.set(Calendar.SECOND, 0);
        }
        return c.getTime();
    }

}
