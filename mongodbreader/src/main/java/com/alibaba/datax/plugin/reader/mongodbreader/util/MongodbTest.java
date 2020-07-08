package com.alibaba.datax.plugin.reader.mongodbreader.util;

import org.bson.types.ObjectId;


public class MongodbTest {
    public static void main(String[] args) {
        ObjectId objectId = new ObjectId("5f004d4b50a6a52a5e950d05");
        System.out.println(objectId.getDate());

    }
}
