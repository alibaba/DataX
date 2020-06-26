package com.alibaba.datax.plugin.reader.mongodbreader.util;

import org.bson.Document;
import org.bson.types.ObjectId;

import java.text.SimpleDateFormat;
import java.util.Date;

public class MongoTest {
    public static void main(String[] args) {
        ObjectId objectId = new ObjectId(getObject("5ef47ecaa2d21036dc756c75"));
        Date date = objectId.getDate();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String format1 = format.format(date);
        System.out.println(format1);

    }

    public static String getObject(String objectid) {
        String object = objectid.substring(0, 8);
        object = object + "0000000000000000";
        System.out.println(object);
        return object;
    }
}
