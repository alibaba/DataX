package com.alibaba.datax.plugin.rdbms.test;

import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * @author Jarod.Kong
 */
public class DBUtilTest {
    @Test
    public void getColumnMetaDataTest() {
        Triple<List<String>, List<Integer>, List<String>> metaData = DBUtil.getColumnMetaData(DataBaseType.PostgreSQL,
                "jdbc:postgresql://192.168.235.3:5432/dw",
                "bluemoon", "bluemoon2016#", "bluemoon.a_demo", "desc");
        Assert.assertNotNull(metaData.getLeft());

    }
}
