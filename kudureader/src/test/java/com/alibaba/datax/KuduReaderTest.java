package com.alibaba.datax;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.RangeSplitUtil;
import com.alibaba.datax.plugin.reader.kudureader.ColumnType;
import com.alibaba.datax.plugin.reader.kudureader.Key;
import com.alibaba.datax.plugin.reader.kudureader.KuduReaderErrorcode;
import com.alibaba.datax.plugin.reader.kudureader.KuduReaderHelper;
import org.apache.commons.lang3.StringUtils;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduPredicate;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * @author daizihao
 * @create 2021-01-21 15:34
 **/
public class KuduReaderTest {
    @Test
    public void getColumnNamesTest() {
        String json = "{\n" +
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
    public void doAsciiStringSplitTest() {
        System.out.println(Arrays.toString(RangeSplitUtil.doAsciiStringSplit("aaa", "eee", 2)));
    }


    private static final Logger LOG = LoggerFactory.getLogger(KuduReaderTest.class);

    @Test
    public void whereTest() {
        String whereSql = "a  >=   1 and  b > 1 and c = 2 and d <> 3 and e != 4 and f is null and g is not null";

        if (whereSql == null || "".equals(whereSql.trim())) {
            return;
        }
        String[] expressions = whereSql.split("and");

        for (String expression : expressions) {
            System.out.println(expression);
            String[] ors = expression.split("or");
            Type type = null;
            Object value = null;
            if (ors.length == 1) {
                String exp = ors[0];
                String[] words = exp.split("\\s+");
                if ("".equals(words[0])){
                    words = Arrays.copyOfRange(words, 1, words.length);
                }
                switch (words[1].charAt(0)) {
                    case '=':
                        LOG.info("The filtering condition is [{} EQUAL(=) {}]", words[0], words[2]);
                        break;
                    case '<':
                        if (words[1].length() == 1) {
                            LOG.info("The filtering condition is [{} LESS(<) {}]", words[0], words[2]);
                        } else if ("<>".equals(words[1])) {
                            LOG.info("The filtering condition is [{} NOT EQUAL(<>) {}]", words[0], words[2]);
                        } else if ("<=".equals(words[1])) {
                            LOG.info("The filtering condition is [{} LESS_EQUAL(<=) {}]", words[0], words[2]);
                        } else {
                            LOG.error("Unsupported where expressions", DataXException.asDataXException(KuduReaderErrorcode.SPLIT_ERROR, "Unsupported where expressions"));
                        }

                        break;
                    case '>':
                        if (words[1].length() == 1) {
                            LOG.info("The filtering condition is [{} GREATER(>) {}]", words[0], words[2]);
                        } else if (">=".equals(words[1])) {
                            LOG.info("The filtering condition is [{} GREATER_EQUAL(>=) {}]", words[0], words[2]);
                        } else {
                            LOG.error("Unsupported where expressions", DataXException.asDataXException(KuduReaderErrorcode.SPLIT_ERROR, "Unsupported where expressions"));
                        }
                        break;
                    case 'i':
                        if ("is".equals(words[1]) && "not".equals(words[2]) && "null".equals(words[3])) {
                            LOG.info("The filtering condition is [{} IS NOT {}]", words[0], "null");
                        } else if ("is".equals(words[1]) && "null".equals(words[2])) {
                            LOG.info("The filtering condition is [{} IS {}]", words[0], "null");
                        } else {
                            LOG.error("Unsupported where expressions", DataXException.asDataXException(KuduReaderErrorcode.SPLIT_ERROR, "Unsupported where expressions"));
                        }

                        break;
                    case '!':
                        if ("!=".equals(words[1])) {
                            LOG.info("The filtering condition is [{} NOT EQUAL(!=) {}]", words[0], words[2]);
                        } else {
                            LOG.error("Unsupported where expressions", DataXException.asDataXException(KuduReaderErrorcode.SPLIT_ERROR, "Unsupported where expressions"));
                        }
                        break;
                    default:
                        LOG.error("Unsupported where expressions", DataXException.asDataXException(KuduReaderErrorcode.SPLIT_ERROR, "Unsupported where expressions"));
                        break;
                }
            }


        }

    }


    @Test
    public void t(){
        System.out.println("a = '1' and b = \"a\" ".replaceAll("[\"']", ""));
    }
}