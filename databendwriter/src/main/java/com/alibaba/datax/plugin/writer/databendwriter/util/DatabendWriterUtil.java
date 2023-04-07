package com.alibaba.datax.plugin.writer.databendwriter.util;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.datax.plugin.rdbms.writer.Key;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.StringJoiner;

public final class DatabendWriterUtil
{
    private static final Logger LOG = LoggerFactory.getLogger(DatabendWriterUtil.class);

    private DatabendWriterUtil() {}
    public static void dealWriteMode(Configuration originalConfig)
    {
        List<String> columns = originalConfig.getList(Key.COLUMN, String.class);

        String jdbcUrl = originalConfig.getString(String.format("%s[0].%s",
                Constant.CONN_MARK, Key.JDBC_URL, String.class));

        String writeMode = originalConfig.getString(Key.WRITE_MODE, "INSERT");

        StringBuilder writeDataSqlTemplate = new StringBuilder();
        writeDataSqlTemplate.append("INSERT INTO %s");
        StringJoiner columnString = new StringJoiner(",");

        for (String column : columns) {
            columnString.add(column);
        }
        writeDataSqlTemplate.append(String.format("(%s)", columnString));
        writeDataSqlTemplate.append(" VALUES");

        LOG.info("Write data [\n{}\n], which jdbcUrl like:[{}]", writeDataSqlTemplate, jdbcUrl);

        originalConfig.set(Constant.INSERT_OR_REPLACE_TEMPLATE_MARK, writeDataSqlTemplate);
    }
}