package com.alibaba.datax.plugin.writer.hdfswriter;

import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class TestHdfsWriter {

    public static void main(String[] args) {
        Properties colProperties = new Properties();

        colProperties.setProperty("columns.types", "int,string,string,string,string,string,string,decimal(12,2),decimal,string,string,string,int,string,string,string,string,decimal,decimal,decimal,int,string,string,string,string,string,int,int,int,int,string,string,decimal,decimal,string,string,string");

        final String columnTypeProperty = colProperties.getProperty(IOConstants.COLUMNS_TYPES);

        List<TypeInfo> columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);

        System.out.println(columnTypes);
    }

}
