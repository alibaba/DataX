package com.alibaba.datax;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.RangeSplitUtil;
import com.alibaba.datax.plugin.reader.kudureader.Key;
import com.alibaba.datax.plugin.reader.kudureader.KuduReaderHelper;
import org.junit.Test;

import java.util.Arrays;

/**
 * @author daizihao
 * @create 2021-01-21 15:34
 **/
public class KuduReaderTest {
    @Test
    public void getColumnNamesTest(){
        String json ="{\n" +
                "  \"column\": [" +
                "    {\n" +
                "      \"name\": \"0\",\n" +
                "      \"type\": \"long\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"1\",\n" +
                "      \"type\": \"boolean\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"type\": \"string\",\n" +
                "      \"name\": \"hello\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"2\",\n" +
                "      \"type\": \"double\"\n" +
                "    }\n" +
                "  ],\n" +
                "  \"fileType\": \"orc\",\n" +
                "  \"encoding\": \"UTF-8\",\n" +
                "  \"fieldDelimiter\": \",\"\n" +
                "}";
        Configuration conf = Configuration.from(json);

        System.out.println(KuduReaderHelper.getColumnNames(conf));
    }

    @Test
    public void doAsciiStringSplitTest(){
        System.out.println(Arrays.toString(RangeSplitUtil.doAsciiStringSplit("aaa", "eee", 2)));
    }


}
