package com.alibaba.datax.plugin.reader.oceanbasev10reader.util;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ObReaderUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ObReaderUtils.class);
    private static final String MYSQL_KEYWORDS = "ACCESSIBLE,ACCOUNT,ACTION,ADD,AFTER,AGAINST,AGGREGATE,ALGORITHM,ALL,ALTER,ALWAYS,ANALYSE,AND,ANY,AS,ASC,ASCII,ASENSITIVE,AT,AUTO_INCREMENT,AUTOEXTEND_SIZE,AVG,AVG_ROW_LENGTH,BACKUP,BEFORE,BEGIN,BETWEEN,BIGINT,BINARY,BINLOG,BIT,BLOB,BLOCK,BOOL,BOOLEAN,BOTH,BTREE,BY,BYTE,CACHE,CALL,CASCADE,CASCADED,CASE,CATALOG_NAME,CHAIN,CHANGE,CHANGED,CHANNEL,CHAR,CHARACTER,CHARSET,CHECK,CHECKSUM,CIPHER,CLASS_ORIGIN,CLIENT,CLOSE,COALESCE,CODE,COLLATE,COLLATION,COLUMN,COLUMN_FORMAT,COLUMN_NAME,COLUMNS,COMMENT,COMMIT,COMMITTED,COMPACT,COMPLETION,COMPRESSED,COMPRESSION,CONCURRENT,CONDITION,CONNECTION,CONSISTENT,CONSTRAINT,CONSTRAINT_CATALOG,CONSTRAINT_NAME,CONSTRAINT_SCHEMA,CONTAINS,CONTEXT,CONTINUE,CONVERT,CPU,CREATE,CROSS,CUBE,CURRENT,CURRENT_DATE,CURRENT_TIME,CURRENT_TIMESTAMP,CURRENT_USER,CURSOR,CURSOR_NAME,DATA,DATABASE,DATABASES,DATAFILE,DATE,DATETIME,DAY,DAY_HOUR,DAY_MICROSECOND,DAY_MINUTE,DAY_SECOND,DEALLOCATE,DEC,DECIMAL,DECLARE,DEFAULT,DEFAULT_AUTH,DEFINER,DELAY_KEY_WRITE,DELAYED,DELETE,DES_KEY_FILE,DESC,DESCRIBE,DETERMINISTIC,DIAGNOSTICS,DIRECTORY,DISABLE,DISCARD,DISK,DISTINCT,DISTINCTROW,DIV,DO,DOUBLE,DROP,DUAL,DUMPFILE,DUPLICATE,DYNAMIC,EACH,ELSE,ELSEIF,ENABLE,ENCLOSED,ENCRYPTION,END,ENDS,ENGINE,ENGINES,ENUM,ERROR,ERRORS,ESCAPE,ESCAPED,EVENT,EVENTS,EVERY,EXCHANGE,EXECUTE,EXISTS,EXIT,EXPANSION,EXPIRE,EXPLAIN,EXPORT,EXTENDED,EXTENT_SIZE,FAST,FAULTS,FETCH,FIELDS,FILE,FILE_BLOCK_SIZE,FILTER,FIRST,FIXED,FLOAT,FLOAT4,FLOAT8,FLUSH,FOLLOWS,FOR,FORCE,FOREIGN,FORMAT,FOUND,FROM,FULL,FULLTEXT,FUNCTION,GENERAL,GENERATED,GEOMETRY,GEOMETRYCOLLECTION,GET,GET_FORMAT,GLOBAL,GRANT,GRANTS,GROUP,GROUP_REPLICATION,HANDLER,HASH,HAVING,HELP,HIGH_PRIORITY,HOST,HOSTS,HOUR,HOUR_MICROSECOND,HOUR_MINUTE,HOUR_SECOND,IDENTIFIED,IF,IGNORE,IGNORE_SERVER_IDS,IMPORT,IN,INDEX,INDEXES,INFILE,INITIAL_SIZE,INNER,INOUT,INSENSITIVE,INSERT,INSERT_METHOD,INSTALL,INSTANCE,INT,INT1,INT2,INT3,INT4,INT8,INTEGER,INTERVAL,INTO,INVOKER,IO,IO_AFTER_GTIDS,IO_BEFORE_GTIDS,IO_THREAD,IPC,IS,ISOLATION,ISSUER,ITERATE,JOIN,JSON,KEY,KEY_BLOCK_SIZE,KEYS,KILL,LANGUAGE,LAST,LEADING,LEAVE,LEAVES,LEFT,LESS,LEVEL,LIKE,LIMIT,LINEAR,LINES,LINESTRING,LIST,LOAD,LOCAL,LOCALTIME,LOCALTIMESTAMP,LOCK,LOCKS,LOGFILE,LOGS,LONG,LONGBLOB,LONGTEXT,LOOP,LOW_PRIORITY,MASTER,MASTER_AUTO_POSITION,MASTER_BIND,MASTER_CONNECT_RETRY,MASTER_DELAY,MASTER_HEARTBEAT_PERIOD,MASTER_HOST,MASTER_LOG_FILE,MASTER_LOG_POS,MASTER_PASSWORD,MASTER_PORT,MASTER_RETRY_COUNT,MASTER_SERVER_ID,MASTER_SSL,MASTER_SSL_CA,MASTER_SSL_CAPATH,MASTER_SSL_CERT,MASTER_SSL_CIPHER,MASTER_SSL_CRL,MASTER_SSL_CRLPATH,MASTER_SSL_KEY,MASTER_SSL_VERIFY_SERVER_CERT,MASTER_TLS_VERSION,MASTER_USER,MATCH,MAX_CONNECTIONS_PER_HOUR,MAX_QUERIES_PER_HOUR,MAX_ROWS,MAX_SIZE,MAX_STATEMENT_TIME,MAX_UPDATES_PER_HOUR,MAX_USER_CONNECTIONS,MAXVALUE,MEDIUM,MEDIUMBLOB,MEDIUMINT,MEDIUMTEXT,MEMORY,MERGE,MESSAGE_TEXT,MICROSECOND,MIDDLEINT,MIGRATE,MIN_ROWS,MINUTE,MINUTE_MICROSECOND,MINUTE_SECOND,MOD,MODE,MODIFIES,MODIFY,MONTH,MULTILINESTRING,MULTIPOINT,MULTIPOLYGON,MUTEX,MYSQL_ERRNO,NAME,NAMES,NATIONAL,NATURAL,NCHAR,NDB,NDBCLUSTER,NEVER,NEW,NEXT,NO,NO_WAIT,NO_WRITE_TO_BINLOG,NODEGROUP,NONBLOCKING,NONE,NOT,NULL,NUMBER,NUMERIC,NVARCHAR,OFFSET,OLD_PASSWORD,ON,ONE,ONLY,OPEN,OPTIMIZE,OPTIMIZER_COSTS,OPTION,OPTIONALLY,OPTIONS,OR,ORDER,OUT,OUTER,OUTFILE,OWNER,PACK_KEYS,PAGE,PARSE_GCOL_EXPR,PARSER,PARTIAL,PARTITION,PARTITIONING,PARTITIONS,PASSWORD,PHASE,PLUGIN,PLUGIN_DIR,PLUGINS,POINT,POLYGON,PORT,PRECEDES,PRECISION,PREPARE,PRESERVE,PREV,PRIMARY,PRIVILEGES,PROCEDURE,PROCESSLIST,PROFILE,PROFILES,PROXY,PURGE,QUARTER,QUERY,QUICK,RANGE,READ,READ_ONLY,READ_WRITE,READS,REAL,REBUILD,RECOVER,REDO_BUFFER_SIZE,REDOFILE,REDUNDANT,REFERENCES,REGEXP,RELAY,RELAY_LOG_FILE,RELAY_LOG_POS,RELAY_THREAD,RELAYLOG,RELEASE,RELOAD,REMOVE,RENAME,REORGANIZE,REPAIR,REPEAT,REPEATABLE,REPLACE,REPLICATE_DO_DB,REPLICATE_DO_TABLE,REPLICATE_IGNORE_DB,REPLICATE_IGNORE_TABLE,REPLICATE_REWRITE_DB,REPLICATE_WILD_DO_TABLE,REPLICATE_WILD_IGNORE_TABLE,REPLICATION,REQUIRE,RESET,RESIGNAL,RESTORE,RESTRICT,RESUME,RETURN,RETURNED_SQLSTATE,RETURNS,REVERSE,REVOKE,RIGHT,RLIKE,ROLLBACK,ROLLUP,ROTATE,ROUTINE,ROW,ROW_COUNT,ROW_FORMAT,ROWS,RTREE,SAVEPOINT,SCHEDULE,SCHEMA,SCHEMA_NAME,SCHEMAS,SECOND,SECOND_MICROSECOND,SECURITY,SELECT,SENSITIVE,SEPARATOR,SERIAL,SERIALIZABLE,SERVER,SESSION,SET,SHARE,SHOW,SHUTDOWN,SIGNAL,SIGNED,SIMPLE,SLAVE,SLOW,SMALLINT,SNAPSHOT,SOCKET,SOME,SONAME,SOUNDS,SOURCE,SPATIAL,SPECIFIC,SQL,SQL_AFTER_GTIDS,SQL_AFTER_MTS_GAPS,SQL_BEFORE_GTIDS,SQL_BIG_RESULT,SQL_BUFFER_RESULT,SQL_CACHE,SQL_CALC_FOUND_ROWS,SQL_NO_CACHE,SQL_SMALL_RESULT,SQL_THREAD,SQL_TSI_DAY,SQL_TSI_HOUR,SQL_TSI_MINUTE,SQL_TSI_MONTH,SQL_TSI_QUARTER,SQL_TSI_SECOND,SQL_TSI_WEEK,SQL_TSI_YEAR,SQLEXCEPTION,SQLSTATE,SQLWARNING,SSL,STACKED,START,STARTING,STARTS,STATS_AUTO_RECALC,STATS_PERSISTENT,STATS_SAMPLE_PAGES,STATUS,STOP,STORAGE,STORED,STRAIGHT_JOIN,STRING,SUBCLASS_ORIGIN,SUBJECT,SUBPARTITION,SUBPARTITIONS,SUPER,SUSPEND,SWAPS,SWITCHES,TABLE,TABLE_CHECKSUM,TABLE_NAME,TABLES,TABLESPACE,TEMPORARY,TEMPTABLE,TERMINATED,TEXT,THAN,THEN,TIME,TIMESTAMP,TIMESTAMPADD,TIMESTAMPDIFF,TINYBLOB,TINYINT,TINYTEXT,TO,TRAILING,TRANSACTION,TRIGGER,TRIGGERS,TRUNCATE,TYPE,TYPES,UNCOMMITTED,UNDEFINED,UNDO,UNDO_BUFFER_SIZE,UNDOFILE,UNICODE,UNINSTALL,UNION,UNIQUE,UNKNOWN,UNLOCK,UNSIGNED,UNTIL,UPDATE,UPGRADE,USAGE,USE,USE_FRM,USER,USER_RESOURCES,USING,UTC_DATE,UTC_TIME,UTC_TIMESTAMP,VALIDATION,VALUE,VALUES,VARBINARY,VARCHAR,VARCHARACTER,VARIABLES,VARYING,VIEW,VIRTUAL,WAIT,WARNINGS,WEEK,WEIGHT_STRING,WHEN,WHERE,WHILE,WITH,WITHOUT,WORK,WRAPPER,WRITE,X509,XA,XID,XML,XOR,YEAR,YEAR_MONTH,ZEROFILL,FALSE,TRUE";
    private static final String ORACLE_KEYWORDS = "ACCESS,ADD,ALL,ALTER,AND,ANY,ARRAYLEN,AS,ASC,AUDIT,BETWEEN,BY,CHAR,CHECK,CLUSTER,COLUMN,COMMENT,COMPRESS,CONNECT,CREATE,CURRENT,DATE,DECIMAL,DEFAULT,DELETE,DESC,DISTINCT,DROP,ELSE,EXCLUSIVE,EXISTS,FILE,FLOAT,FOR,FROM,GRANT,GROUP,HAVING,IDENTIFIED,IMMEDIATE,IN,INCREMENT,INDEX,INITIAL,INSERT,INTEGER,INTERSECT,INTO,IS,LEVEL,LIKE,LOCK,LONG,MAXEXTENTS,MINUS,MODE,MODIFY,NOAUDIT,NOCOMPRESS,NOT,NOTFOUND,NOWAIT,NULL,NUMBER,OF,OFFLINE,ON,ONLINE,OPTION,OR,ORDER,PCTFREE,PRIOR,PRIVILEGES,PUBLIC,RAW,RENAME,RESOURCE,REVOKE,ROW,ROWID,ROWLABEL,ROWNUM,ROWS,SELECT,SESSION,SET,SHARE,SIZE,SMALLINT,SQLBUF,START,SUCCESSFUL,SYNONYM,TABLE,THEN,TO,TRIGGER,UID,UNION,UNIQUE,UPDATE,USER,VALIDATE,VALUES,VARCHAR,VARCHAR2,VIEW,WHENEVER,WHERE,WITH";

    private static Set<String> databaseKeywords;
    final static public String OB_COMPATIBLE_MODE = "obCompatibilityMode";
    final static public String OB_COMPATIBLE_MODE_ORACLE = "ORACLE";
    final static public String OB_COMPATIBLE_MODE_MYSQL = "MYSQL";

    public static String compatibleMode = OB_COMPATIBLE_MODE_MYSQL;

    public static final DataBaseType databaseType = DataBaseType.OceanBase;


    private static Set<String> keywordsFromString2HashSet(final String keywords) {
        return new HashSet(Arrays.asList(keywords.split(",")));
    }

    public static String escapeDatabaseKeywords(String keyword) {
        if (databaseKeywords == null) {
            if (isOracleMode(compatibleMode)) {
                databaseKeywords = keywordsFromString2HashSet(ORACLE_KEYWORDS);
            } else {
                databaseKeywords = keywordsFromString2HashSet(MYSQL_KEYWORDS);
            }
        }
        char escapeChar = isOracleMode(compatibleMode) ? '"' : '`';
        if (databaseKeywords.contains(keyword.toUpperCase())) {
            keyword = escapeChar + keyword + escapeChar;
        }
        return keyword;
    }

    public static void escapeDatabaseKeywords(List<String> keywords) {
        for (int i = 0; i < keywords.size(); i++) {
            keywords.set(i, escapeDatabaseKeywords(keywords.get(i)));
        }
    }

    public static Boolean isEscapeMode(String keyword) {
        if (isOracleMode(compatibleMode)) {
            return keyword.startsWith("\"") && keyword.endsWith("\"");
        } else {
            return keyword.startsWith("`") && keyword.endsWith("`");
        }
    }

    public static void initConn4Reader(Connection conn, long queryTimeoutSeconds) {
        String setQueryTimeout = "set ob_query_timeout=" + (queryTimeoutSeconds * 1000 * 1000L);
        String setTrxTimeout = "set ob_trx_timeout=" + ((queryTimeoutSeconds + 5) * 1000 * 1000L);
        Statement stmt = null;
        try {
            conn.setAutoCommit(true);
            stmt = conn.createStatement();
            stmt.execute(setQueryTimeout);
            stmt.execute(setTrxTimeout);
            LOG.warn("setAutoCommit=true;" + setQueryTimeout + ";" + setTrxTimeout + ";");
        } catch (Throwable e) {
            LOG.warn("initConn4Reader fail", e);
        } finally {
            DBUtil.closeDBResources(stmt, null);
        }
    }

    public static void sleep(int ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
        }
    }

    /**
     * @param conn
     * @param context
     */
    public static void matchPkIndexs(Connection conn, TaskContext context) {
        String[] pkColumns = getPkColumns(conn, context);
        if (ArrayUtils.isEmpty(pkColumns)) {
            LOG.warn("table=" + context.getTable() + " has no primary key");
            return;
        }
        List<String> columns = context.getColumns();

        // 最后参与排序的索引列

        context.setPkColumns(pkColumns);

        int[] pkIndexs = new int[pkColumns.length];
        for (int i = 0, n = pkColumns.length; i < n; i++) {
            String pkc = pkColumns[i];
            int j = 0;
            for (int k = columns.size(); j < k; j++) {
                // 如果用户定义的 columns中 带有 ``,也不影响,
                // 最多只是在select里多加了几列PK column
                if (StringUtils.equalsIgnoreCase(pkc, columns.get(j))) {
                    pkIndexs[i] = j;
                    break;
                }
            }
            // 到这里 说明主键列不在columns中,则主动追加到尾部
            if (j == columns.size()) {
                columns.add(pkc);
                pkIndexs[i] = columns.size() - 1;
            }
        }
        context.setPkIndexs(pkIndexs);
    }

    private static String[] getPkColumns(Connection conn, TaskContext context) {
        String tableName = context.getTable();
        String sql = "show index from " + tableName + " where Key_name='PRIMARY'";
        if (isOracleMode(context.getCompatibleMode())) {
            tableName = tableName.toUpperCase();
            sql = "SELECT cols.column_name Column_name " +
                    "FROM all_constraints cons, all_cons_columns cols " +
                    "WHERE cols.table_name = '" + tableName + "' AND cons.constraint_type = 'P' " +
                    "AND cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner";
        }
        LOG.info("get primary key by sql: " + sql);
        Statement ps = null;
        ResultSet rs = null;
        List<String> realIndex = new ArrayList<String>();
        realIndex.addAll(context.getSecondaryIndexColumns());
        try {
            ps = conn.createStatement();
            rs = ps.executeQuery(sql);
            while (rs.next()) {
                String columnName = rs.getString("Column_name");
                columnName = escapeDatabaseKeywords(columnName);
                if (!isEscapeMode(columnName)) {
                    columnName.toLowerCase();
                }
                if (!realIndex.contains(columnName)) {
                    realIndex.add(columnName);
                }
            }

            String[] pks = new String[realIndex.size()];
            realIndex.toArray(pks);
            return pks;
        } catch (Throwable e) {
            LOG.error("show index from table fail :" + sql, e);
        } finally {
            close(rs, ps, null);
        }
        return null;
    }

    /**
     * 首次查的SQL
     *
     * @param context
     * @return
     */
    public static String buildFirstQuerySql(TaskContext context) {
        String userSavePoint = context.getUserSavePoint();
        String indexName = context.getIndexName();
        String sql = "select ";
        boolean weakRead = context.getWeakRead();
        if (StringUtils.isNotEmpty(indexName)) {
            String weakReadHint = weakRead ? "+READ_CONSISTENCY(WEAK)," : "+";
            sql += " /*" + weakReadHint + "index(" + context.getTable() + " " + indexName + ")*/ ";
        } else if (weakRead) {
            sql += " /*+READ_CONSISTENCY(WEAK)*/ ";
        }
        sql += StringUtils.join(context.getColumns(), ',');
        sql += " from " + context.getTable();
        if (context.getPartitionName() != null) {
            sql += String.format(" partition(%s) ", context.getPartitionName());
        }
        if (StringUtils.isNotEmpty(context.getWhere())) {
            sql += " where " + context.getWhere();
        }

        if (userSavePoint != null && userSavePoint.length() != 0) {
            userSavePoint = userSavePoint.replace("=", ">");
            sql += (StringUtils.isNotEmpty(context.getWhere()) ? " and " : " where ") + userSavePoint;
        }

        sql += " order by " + StringUtils.join(context.getPkColumns(), ',') + " asc";

        // Using sub-query to apply rownum < readBatchSize since where has higher priority than order by
        if (ObReaderUtils.isOracleMode(context.getCompatibleMode()) && context.getReadBatchSize() != -1) {
            sql = String.format("select * from (%s) where rownum <= %d", sql, context.getReadBatchSize());
        }

        return sql;
    }

    /**
     * 增量查的SQL
     *
     * @param conn
     * @param context
     * @return sql
     */
    public static String buildAppendQuerySql(Connection conn, TaskContext context) {
        String indexName = context.getIndexName();
        boolean weakRead = context.getWeakRead();
        String sql = "select ";
        if (StringUtils.isNotEmpty(indexName)) {
            String weakReadHint = weakRead ? "+READ_CONSISTENCY(WEAK)," : "+";
            sql += " /*" + weakReadHint + "index(" + context.getTable() + " " + indexName + ")*/ ";
        } else if (weakRead) {
            sql += " /*+READ_CONSISTENCY(WEAK)*/ ";
        }
        sql += StringUtils.join(context.getColumns(), ',') + " from " + context.getTable();

        if (context.getPartitionName() != null) {
            sql += String.format(" partition(%s) ", context.getPartitionName());
        }

        sql += " where ";
        String append = "(" + StringUtils.join(context.getPkColumns(), ',') + ") > ("
                + buildPlaceHolder(context.getPkColumns().length) + ")";

        if (StringUtils.isNotEmpty(context.getWhere())) {
            sql += "(" + context.getWhere() + ") and ";
        }

        sql = String.format("%s %s order by %s asc", sql, append, StringUtils.join(context.getPkColumns(), ','));

        // Using sub-query to apply rownum < readBatchSize since where has higher priority than order by
        if (ObReaderUtils.isOracleMode(context.getCompatibleMode()) && context.getReadBatchSize() != -1) {
            sql = String.format("select * from (%s) where rownum <= %d", sql, context.getReadBatchSize());
        }

        return sql;
    }

    /**
     * check if the userSavePoint is valid
     *
     * @param context
     * @return true - valid, false - invalid
     */
    public static boolean isUserSavePointValid(TaskContext context) {
        String userSavePoint = context.getUserSavePoint();
        if (userSavePoint == null || userSavePoint.length() == 0) {
            LOG.info("user save point is empty!");
            return false;
        }

        LOG.info("validating user save point: " + userSavePoint);

        final String patternString = "(.+)=(.+)";
        Pattern parttern = Pattern.compile(patternString);
        Matcher matcher = parttern.matcher(userSavePoint);
        if (!matcher.find()) {
            LOG.error("user save point format is not correct: " + userSavePoint);
            return false;
        }

        List<String> columnsInUserSavePoint = getColumnsFromUserSavePoint(userSavePoint);
        List<String> valuesInUserSavePoint = getValuesFromUserSavePoint(userSavePoint);
        if (columnsInUserSavePoint.size() == 0 || valuesInUserSavePoint.size() == 0 ||
                columnsInUserSavePoint.size() != valuesInUserSavePoint.size()) {
            LOG.error("number of columns and values in user save point are different:" + userSavePoint);
            return false;
        }

        String where = context.getWhere();
        if (StringUtils.isNotEmpty(where)) {
            for (String column : columnsInUserSavePoint) {
                if (where.contains(column)) {
                    LOG.error("column " + column + " is conflict with where: " + where);
                    return false;
                }
            }
        }

        // Columns in userSavePoint must be the selected index.
        String[] pkColumns = context.getPkColumns();
        if (pkColumns.length != columnsInUserSavePoint.size()) {
            LOG.error("user save point is not on the selected index.");
            return false;
        }

        for (String column : columnsInUserSavePoint) {
            boolean found = false;
            for (String pkCol : pkColumns) {
                if (pkCol.equals(column)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                LOG.error("column " + column + " is not on the selected index.");
                return false;
            }
        }

        return true;
    }

    private static String removeBracket(String str) {
        final char leftBracket = '(';
        final char rightBracket = ')';
        if (str != null && str.contains(String.valueOf(leftBracket)) && str.contains(String.valueOf(rightBracket)) &&
                str.indexOf(leftBracket) < str.indexOf(rightBracket)) {
            return str.substring(str.indexOf(leftBracket) + 1, str.indexOf(rightBracket));
        }
        return str;
    }

    private static List<String> getColumnsFromUserSavePoint(String userSavePoint) {
        return Arrays.asList(removeBracket(userSavePoint.split("=")[0]).split(","));
    }

    private static List<String> getValuesFromUserSavePoint(String userSavePoint) {
        return Arrays.asList(removeBracket(userSavePoint.split("=")[1]).split(","));
    }

    /**
     * 先解析成where
     * <p>
     * 再判断是否存在索引
     *
     * @param conn
     * @param context
     * @return
     */
    public static void initIndex(Connection conn, TaskContext context) {
        if (StringUtils.isEmpty(context.getWhere())) {
            return;
        }
        SQLExpr expr = SQLUtils.toSQLExpr(context.getWhere(), "mysql");
        LOG.info("expr: " + expr);
        List<String> allColumnsInTab = getAllColumnFromTab(conn, context.getTable());
        List<String> allColNames = getColNames(allColumnsInTab, expr);

        if (allColNames == null) {
            return;
        }

        // Remove the duplicated column names
        Set<String> colNames = new TreeSet<String>();
        for (String colName : allColNames) {
            if (!colNames.contains(colName)) {
                colNames.add(colName);
            }
        }
        List<String> indexNames = getIndexName(conn, context.getTable(), colNames, context.getCompatibleMode());
        findBestIndex(conn, indexNames, context.getTable(), context);
    }

    private static List<String> getAllColumnFromTab(Connection conn, String tableName) {
        String sql = "show columns from " + tableName;
        Statement stmt = null;
        ResultSet rs = null;
        List<String> allColumns = new ArrayList<String>();
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                allColumns.add(rs.getString("Field").toUpperCase());
            }
        } catch (Exception e) {
            LOG.warn("fail to get all columns from table " + tableName, e);
        } finally {
            close(rs, stmt, null);
        }

        LOG.info("all columns in tab: " + String.join(",", allColumns));
        return allColumns;
    }

    /**
     * 找出where条件中的列名，目前仅支持全部为and条件，并且操作符为大于、大约等于、等于、小于、小于等于和不等于的表达式。
     * <p>
     * test coverage: - c6 = 20180710 OR c4 = 320: no index selected - 20180710
     * = c6: correct index selected - 20180710 = c6 and c4 = 320 or c2 < 100: no
     * index selected
     *
     * @param expr
     * @return
     */
    private static List<String> getColNames(List<String> allColInTab, SQLExpr expr) {
        List<String> colNames = new ArrayList<String>();
        if (expr instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr exp = (SQLBinaryOpExpr) expr;
            if (exp.getOperator() == SQLBinaryOperator.BooleanAnd) {
                List<String> leftColumns = getColNames(allColInTab, exp.getLeft());
                List<String> rightColumns = getColNames(allColInTab, exp.getRight());
                if (leftColumns == null || rightColumns == null) {
                    return null;
                }
                colNames.addAll(leftColumns);
                colNames.addAll(rightColumns);
            } else if (exp.getOperator() == SQLBinaryOperator.GreaterThan
                    || exp.getOperator() == SQLBinaryOperator.GreaterThanOrEqual
                    || exp.getOperator() == SQLBinaryOperator.Equality
                    || exp.getOperator() == SQLBinaryOperator.LessThan
                    || exp.getOperator() == SQLBinaryOperator.LessThanOrEqual
                    || exp.getOperator() == SQLBinaryOperator.NotEqual) {
                // only support simple comparison operators
                String left = SQLUtils.toMySqlString(exp.getLeft()).toUpperCase();
                String right = SQLUtils.toMySqlString(exp.getRight()).toUpperCase();
                LOG.debug("left: " + left + ", right: " + right);
                if (allColInTab.contains(left)) {
                    colNames.add(left);
                }

                if (allColInTab.contains(right)) {
                    colNames.add(right);
                }
            } else {
                // unsupported operators
                return null;
            }
        }

        return colNames;
    }

    private static Map<String, List<String>> getAllIndex(Connection conn, String tableName, String compatibleMode) {
        Map<String, List<String>> allIndex = new HashMap<String, List<String>>();
        String sql = "show index from " + tableName;
        if (isOracleMode(compatibleMode)) {
            tableName = tableName.toUpperCase();
            sql = "SELECT INDEX_NAME Key_name, COLUMN_NAME Column_name " +
                    "from dba_ind_columns where TABLE_NAME = '" + tableName + "' " +
                    " union all " +
                    "SELECT DISTINCT " +
                    "CASE " +
                    "WHEN cons.CONSTRAINT_TYPE = 'P' THEN 'PRIMARY' " +
                    "WHEN cons.CONSTRAINT_TYPE = 'U' THEN cons.CONSTRAINT_NAME " +
                    "ELSE '' " +
                    "END AS Key_name, " +
                    "cols.column_name Column_name " +
                    "FROM all_constraints cons, all_cons_columns cols " +
                    "WHERE cols.table_name = '" + tableName + "' AND cons.constraint_type in('P', 'U') " +
                    "AND cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner";
        }
        Statement stmt = null;
        ResultSet rs = null;

        try {
            LOG.info("running sql to get index: " + sql);
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                String keyName = rs.getString("Key_name");
                String colName = rs.getString("Column_name").toUpperCase();
                if (allIndex.containsKey(keyName)) {
                    allIndex.get(keyName).add(colName);
                } else {
                    List<String> allColumns = new ArrayList<String>();
                    allColumns.add(colName);
                    allIndex.put(keyName, allColumns);
                }
            }

            // add primary key to all index
            if (allIndex.containsKey("PRIMARY")) {
                List<String> colsInPrimary = allIndex.get("PRIMARY");
                for (String keyName : allIndex.keySet()) {
                    if (keyName.equals("PRIMARY")) {
                        continue;
                    }
                    allIndex.get(keyName).addAll(colsInPrimary);
                }
            }
        } catch (Exception e) {
            LOG.error("fail to get all keys from table" + sql, e);
        } finally {
            close(rs, stmt, null);
        }

        LOG.info("all index: " + allIndex.toString());
        return allIndex;
    }

    /**
     * @param conn
     * @param table
     * @param colNamesInCondition
     * @return
     */
    private static List<String> getIndexName(Connection conn, String table,
                                             Set<String> colNamesInCondition, String compatibleMode) {
        List<String> indexNames = new ArrayList<String>();
        if (colNamesInCondition == null || colNamesInCondition.size() == 0) {
            LOG.info("there is no qulified conditions in the where clause, skip index selection.");
            return indexNames;
        }

        LOG.info("columNamesInConditions: " + String.join(",", colNamesInCondition));

        Map<String, List<String>> allIndex = getAllIndex(conn, table, compatibleMode);
        for (String keyName : allIndex.keySet()) {
            boolean indexNotMatch = false;
            // If the index does not have all the column in where conditions, it
            // can not be chosen
            // the selected index must start with the columns in where condition
            if (allIndex.get(keyName).size() < colNamesInCondition.size()) {
                indexNotMatch = true;
            } else {
                // the the first number columns of this index
                int num = colNamesInCondition.size();
                for (String colName : allIndex.get(keyName)) {
                    if (!colNamesInCondition.contains(colName)) {
                        indexNotMatch = true;
                        break;
                    }
                    if (--num == 0) {
                        break;
                    }
                }
            }

            if (indexNotMatch) {
                continue;
            } else {
                indexNames.add(keyName);
            }
        }

        return indexNames;
    }

    /**
     * 以 column开头的索引,可能有多个,也可能存在多列的情形
     * <p>
     * 所以,需要选择列数最少的
     *
     * @param indexNames
     * @param context
     */
    private static void findBestIndex(Connection conn, List<String> indexNames, String table, TaskContext context) {
        if (indexNames.size() == 0) {
            LOG.warn("table has no index.");
            return;
        }

        Map<String, Map<Integer, String>> allIndexs = new HashMap<String, Map<Integer, String>>();
        String sql = "show index from " + table + " where key_name in (" + buildPlaceHolder(indexNames.size()) + ")";
        if (isOracleMode(context.getCompatibleMode())) {
            Map<String, List<String>> allIndexInTab = getAllIndex(conn, table, context.getCompatibleMode());
            for (String indexName : indexNames) {
                if (allIndexInTab.containsKey(indexName)) {
                    Map<Integer, String> index = new TreeMap<Integer, String>();
                    List<String> columnList = allIndexInTab.get(indexName);
                    for (int i = 1; i <= columnList.size(); i++) {
                        index.put(i, columnList.get(i - 1));
                    }
                    allIndexs.put(indexName, index);
                } else {
                    LOG.error("index does not exist: " + indexName);
                }
            }
        } else {
            PreparedStatement ps = null;
            ResultSet rs = null;
            try {
                ps = conn.prepareStatement(sql);
                for (int i = 0, n = indexNames.size(); i < n; i++) {
                    ps.setString(i + 1, indexNames.get(i));
                }
                rs = ps.executeQuery();
                while (rs.next()) {
                    String keyName = rs.getString("Key_name");
                    Map<Integer, String> index = allIndexs.get(keyName);
                    if (index == null) {
                        index = new TreeMap<Integer, String>();
                        allIndexs.put(keyName, index);
                    }
                    int keyInIndex = rs.getInt("Seq_in_index");
                    String column = rs.getString("Column_name");
                    index.put(keyInIndex, column);
                }
            } catch (Throwable e) {
                LOG.error("show index from table fail :" + sql, e);
            } finally {
                close(rs, ps, null);
            }
        }

        LOG.info("possible index:" + allIndexs + ",where:" + context.getWhere());

        Entry<String, Map<Integer, String>> chooseIndex = null;
        int columnCount = Integer.MAX_VALUE;
        for (Entry<String, Map<Integer, String>> entry : allIndexs.entrySet()) {
            if (entry.getValue().size() < columnCount) {
                columnCount = entry.getValue().size();
                chooseIndex = entry;
            }
        }

        if (chooseIndex != null) {
            LOG.info("choose index name:" + chooseIndex.getKey() + ",columns:" + chooseIndex.getValue());
            context.setIndexName(chooseIndex.getKey());
            context.setSecondaryIndexColumns(new ArrayList<String>(chooseIndex.getValue().values()));
        }
    }

    /**
     * 由于ObProxy存在bug,事务超时或事务被杀时,conn的close是没有响应的
     *
     * @param rs
     * @param stmt
     * @param conn
     */
    public static void close(final ResultSet rs, final Statement stmt, final Connection conn) {
        DBUtil.closeDBResources(rs, stmt, conn);
    }

    /**
     * 判断是否重复record
     *
     * @param savePoint
     * @param row
     * @param pkIndexs
     * @return
     */
    public static boolean isPkEquals(Record savePoint, Record row, int[] pkIndexs) {
        if (savePoint == null || row == null) {
            return false;
        }
        try {
            for (int index : pkIndexs) {
                Object left = savePoint.getColumn(index).getRawData();
                Object right = row.getColumn(index).getRawData();
                if (!left.equals(right)) {
                    return false;
                }
            }
        } catch (Throwable e) {
            return false;
        }
        return true;
    }

    public static String buildPlaceHolder(int n) {
        if (n <= 0) {
            return "";
        }
        StringBuilder str = new StringBuilder(2 * n);
        str.append('?');
        for (int i = 1; i < n; i++) {
            str.append(",?");
        }
        return str.toString();
    }

    public static void binding(PreparedStatement ps, List<Column> list) throws SQLException {
        for (int i = 0, n = list.size(); i < n; i++) {
            Column c = list.get(i);
            if (c instanceof BoolColumn) {
                ps.setLong(i + 1, ((BoolColumn) c).asLong());
            } else if (c instanceof BytesColumn) {
                ps.setBytes(i + 1, ((BytesColumn) c).asBytes());
            } else if (c instanceof DateColumn) {
                ps.setTimestamp(i + 1, new Timestamp(((DateColumn) c).asDate().getTime()));
            } else if (c instanceof DoubleColumn) {
                ps.setDouble(i + 1, ((DoubleColumn) c).asDouble());
            } else if (c instanceof LongColumn) {
                ps.setLong(i + 1, ((LongColumn) c).asLong());
            } else if (c instanceof StringColumn) {
                ps.setString(i + 1, ((StringColumn) c).asString());
            } else {
                ps.setObject(i + 1, c.getRawData());
            }
        }
    }

    public static List<Column> buildPoint(Record savePoint, int[] pkIndexs) {
        List<Column> result = new ArrayList<Column>(pkIndexs.length);
        for (int i = 0, n = pkIndexs.length; i < n; i++) {
            result.add(savePoint.getColumn(pkIndexs[i]));
        }
        return result;
    }

    public static String getCompatibleMode(Connection conn) {
        String compatibleMode = OB_COMPATIBLE_MODE_MYSQL;
        String getCompatibleModeSql = "SHOW VARIABLES LIKE 'ob_compatibility_mode'";
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(getCompatibleModeSql);
            if (rs.next()) {
                compatibleMode = rs.getString("VALUE");
            }
        } catch (Exception e) {
            LOG.error("fail to get ob compatible mode, using mysql as default: " + e.getMessage());
        } finally {
            DBUtil.closeDBResources(rs, stmt, conn);
        }

        LOG.info("ob compatible mode is " + compatibleMode);
        return compatibleMode;
    }

    public static boolean isOracleMode(String mode) {
        return (mode != null && OB_COMPATIBLE_MODE_ORACLE.equalsIgnoreCase(mode));
    }
}
