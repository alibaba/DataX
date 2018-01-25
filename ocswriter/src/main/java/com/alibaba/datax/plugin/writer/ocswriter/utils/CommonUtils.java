package com.alibaba.datax.plugin.writer.ocswriter.utils;

public class CommonUtils {

    public static void sleepInMs(long time) {
        try{
            Thread.sleep(time);
        } catch (InterruptedException e) {
            //
        }
    }
}
