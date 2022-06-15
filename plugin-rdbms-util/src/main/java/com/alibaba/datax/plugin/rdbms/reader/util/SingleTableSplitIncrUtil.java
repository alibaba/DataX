package com.alibaba.datax.plugin.rdbms.reader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.Constant;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.RdbmsException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;

import static com.alibaba.datax.plugin.rdbms.reader.util.SingleTableSplitUtil.DATABASE_TYPE;

/**
 * @author Jarod.Kong
 */
public class SingleTableSplitIncrUtil {
    private static final Logger LOG = LoggerFactory
            .getLogger(SingleTableSplitIncrUtil.class);

    @SuppressWarnings("resource")
    public static Pair<Object, Object> getMinMaxIncrFieldRange(Configuration configuration) {

        int fetchSize = configuration.getInt(Constant.FETCH_SIZE);
        String jdbcURL = configuration.getString(Key.JDBC_URL);
        String username = configuration.getString(Key.USERNAME);
        String password = configuration.getString(Key.PASSWORD);
        String table = configuration.getString(Key.TABLE);
        String incrPk = configuration.getString(Constant.SPLIT_INCR_FIELD);
        String where = configuration.getString(Key.WHERE, null);
        String incrRangeSQL = SingleTableSplitUtil.genPKSql(incrPk, table, where);

        Connection conn = DBUtil.getConnection(DATABASE_TYPE, jdbcURL, username, password);
        Pair<Object, Object> minMaxIncr = checkSplitIncrField(conn, incrRangeSQL, fetchSize, table, username, configuration);
        DBUtil.closeDBResources(null, null, conn);
        return minMaxIncr;
    }

    /**
     * 检测splitIncr的配置是否正确。
     */
    private static Pair<Object, Object> checkSplitIncrField(Connection conn, String incrRangeSQL, int fetchSize, String table,
                                                            String username, Configuration configuration) {
        LOG.info("split pk [sql={}] is running... ", incrRangeSQL);
        ResultSet rs = null;
        Pair<Object, Object> minMaxPK = null;
        try {
            try {
                rs = DBUtil.query(conn, incrRangeSQL, fetchSize);
            } catch (Exception e) {
                throw RdbmsException.asQueryException(DATABASE_TYPE, e, incrRangeSQL, table, username);
            }
            ResultSetMetaData rsMetaData = rs.getMetaData();
            if (isIncrTypeValid(rsMetaData)) {
                if (isTimestampType(rsMetaData.getColumnType(1))) {
                    if (configuration != null) {
                        configuration
                                .set(Constant.INCR_TYPE, Constant.INCR_TYPE_TIMESTAMP);
                    }
                    while (DBUtil.asyncResultSetNext(rs)) {
                        minMaxPK = new ImmutablePair<Object, Object>(
                                rs.getString(1), rs.getString(2));
                    }
                } else {
                    throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_SPLIT_PK,
                            "您配置的DataX增量字段(splitIncr)有误. 因为您配置的增量字段(splitIncr) 类型 DataX 不支持. DataX 仅支持增量字段为一个,并且类型为整数或者字符串类型. 请尝试使用其他的增量字段或者联系 DBA 进行处理.");
                }
            } else {
                throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_SPLIT_PK,
                        "您配置的DataX增量字段(splitIncr)有误. 因为您配置的增量字段(splitIncr) 类型 DataX 不支持. DataX 仅支持增量字段为一个,并且类型为整数或者字符串类型. 请尝试使用其他的增量字段或者联系 DBA 进行处理.");
            }
        } catch (DataXException e) {
            throw e;
        } catch (Exception e) {
            throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_SPLIT_PK, "DataX尝试获取表增量信息发生错误. 请检查您的配置并作出修改.", e);
        } finally {
            DBUtil.closeDBResources(rs, null, null);
        }

        return minMaxPK;
    }

    private static boolean isIncrTypeValid(ResultSetMetaData rsMetaData) {
        boolean ret = false;
        try {
            int minType = rsMetaData.getColumnType(1);
            int maxType = rsMetaData.getColumnType(2);

            boolean isTimestampType = isTimestampType(minType);

            if (minType == maxType && (isTimestampType)) {
                ret = true;
            }
        } catch (Exception e) {
            throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_SPLIT_PK,
                    "DataX获取增量字段(splitIncr)字段类型失败. 该错误通常是系统底层异常导致. 请联系旺旺:askdatax或者DBA处理.");
        }
        return ret;
    }

    @SuppressWarnings("UnnecessaryLocalVariable")
    private static boolean isTimestampType(int type) {
        boolean isValidTimestamp = type == Types.TIMESTAMP || type == Types.TIMESTAMP_WITH_TIMEZONE
                || type == Types.DATE;
        return isValidTimestamp;
    }

}
