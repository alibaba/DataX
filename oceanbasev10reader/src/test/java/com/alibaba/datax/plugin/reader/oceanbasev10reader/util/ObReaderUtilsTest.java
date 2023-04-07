package com.alibaba.datax.plugin.reader.oceanbasev10reader.util;

import org.junit.Test;

public class ObReaderUtilsTest {

    @Test
    public void getDbTest() {
        assert ObReaderUtils.getDbNameFromJdbcUrl("jdbc:mysql://127.0.0.1:3306/testdb").equalsIgnoreCase("testdb");
        assert ObReaderUtils.getDbNameFromJdbcUrl("jdbc:oceanbase://127.0.0.1:2883/testdb").equalsIgnoreCase("testdb");
        assert ObReaderUtils.getDbNameFromJdbcUrl("||_dsc_ob10_dsc_||obcluster:mysql||_dsc_ob10_dsc_||jdbc:mysql://127.0.0.1:3306/testdb").equalsIgnoreCase("testdb");
        assert ObReaderUtils.getDbNameFromJdbcUrl("||_dsc_ob10_dsc_||obcluster:oracle||_dsc_ob10_dsc_||jdbc:oceanbase://127.0.0.1:3306/testdb").equalsIgnoreCase("testdb");
    }

    @Test
    public void compareObVersionTest() {
        assert ObReaderUtils.compareObVersion("2.2.70", "3.2.2") == -1;
        assert ObReaderUtils.compareObVersion("2.2.70", "2.2.50") == 1;
        assert ObReaderUtils.compareObVersion("2.2.70", "3.1.2") == -1;
        assert ObReaderUtils.compareObVersion("3.1.2", "3.1.2") == 0;
        assert ObReaderUtils.compareObVersion("3.2.3.0", "3.2.3.0") == 0;
        assert ObReaderUtils.compareObVersion("3.2.3.0-CE", "3.2.3.0") == 0;
    }
}
