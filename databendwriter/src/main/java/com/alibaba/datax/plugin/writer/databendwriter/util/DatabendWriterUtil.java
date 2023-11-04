package com.alibaba.datax.plugin.writer.databendwriter.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.datax.plugin.rdbms.writer.Key;

import com.alibaba.datax.plugin.writer.databendwriter.DatabendWriterErrorCode;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.crypto.Data;
import java.util.List;
import java.util.StringJoiner;

public final class DatabendWriterUtil {
    private static final Logger LOG = LoggerFactory.getLogger(DatabendWriterUtil.class);

    private DatabendWriterUtil() {
    }

    public static void dealWriteMode(Configuration originalConfig) throws DataXException {
        List<String> columns = originalConfig.getList(Key.COLUMN, String.class);
        List<String> onConflictColumns = originalConfig.getList(Key.ONCONFLICT_COLUMN, String.class);
        StringBuilder writeDataSqlTemplate = new StringBuilder();

        String jdbcUrl = originalConfig.getString(String.format("%s[0].%s",
                Constant.CONN_MARK, Key.JDBC_URL, String.class));

        String writeMode = originalConfig.getString(Key.WRITE_MODE, "INSERT");
        LOG.info("write mode is {}", writeMode);
        if (writeMode.toLowerCase().contains("replace")) {
            if (onConflictColumns == null || onConflictColumns.size() == 0) {
                throw DataXException
                        .asDataXException(
                                DatabendWriterErrorCode.CONF_ERROR,
                                String.format(
                                        "Replace mode must has onConflictColumn config."
                                ));
            }

            // for databend if you want to use replace mode, the writeMode should be:  "writeMode": "replace"
            writeDataSqlTemplate.append("REPLACE INTO %s (")
                    .append(StringUtils.join(columns, ",")).append(") ").append(onConFlictDoString(onConflictColumns))
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

    public static String onConFlictDoString(List<String> conflictColumns) {
        return " ON " +
                "(" +
                StringUtils.join(conflictColumns, ",") + ") ";
    }
}
