package com.alibaba.datax.plugin.writer.databendwriter.util;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.datax.plugin.rdbms.writer.Key;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.StringJoiner;

public final class DatabendWriterUtil {
    private static final Logger LOG = LoggerFactory.getLogger(DatabendWriterUtil.class);

    private DatabendWriterUtil() {
    }

    public static void dealWriteMode(Configuration originalConfig) {
        List<String> columns = originalConfig.getList(Key.COLUMN, String.class);
        String valueHolders = "?";
        StringBuilder writeDataSqlTemplate = new StringBuilder();

        String jdbcUrl = originalConfig.getString(String.format("%s[0].%s",
                Constant.CONN_MARK, Key.JDBC_URL, String.class));

        String writeMode = originalConfig.getString(Key.WRITE_MODE, "INSERT");
        LOG.info("write mode is {}", writeMode);
        if (writeMode.toLowerCase().contains("replace")) {
            // for databend if you want to use replace mode, the writeMode should be:  "writeMode": "replace (userid)"
            writeDataSqlTemplate.append("REPLACE INTO %s (")
                    .append(StringUtils.join(columns, ",")).append(") ").append(onConFlictDoString(writeMode))
                    .append(" VALUES");

            LOG.info("Replace data [\n{}\n], which jdbcUrl like:[{}]", writeDataSqlTemplate, jdbcUrl);
            originalConfig.set(Constant.INSERT_OR_REPLACE_TEMPLATE_MARK, writeDataSqlTemplate);
        } else {
            writeDataSqlTemplate.append("INSERT INTO %s");
            StringJoiner columnString = new StringJoiner(",");

            for (String column : columns) {
                columnString.add(column);
            }
            writeDataSqlTemplate.append(String.format("(%s)", columnString));
            writeDataSqlTemplate.append(" VALUES");

            LOG.info("Insert data [\n{}\n], which jdbcUrl like:[{}]", writeDataSqlTemplate, jdbcUrl);

            originalConfig.set(Constant.INSERT_OR_REPLACE_TEMPLATE_MARK, writeDataSqlTemplate);
        }
    }

    public static String onConFlictDoString(String conflict) {
        conflict = conflict.replace("replace", "");
        StringBuilder sb = new StringBuilder();
        sb.append(" ON ");
        sb.append(conflict);
        return sb.toString();
    }
}
