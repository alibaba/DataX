package cn.sensorsdata.datax.plugin;

public class KeyConstant {

    /**
     * hive用户名
     */
    public static final String USER_NAME = "username";
    /**
     * hive密码
     */
    public static final String PASSWORD = "password";
    /**
     * 查询的列
     */
    public static final String COLUMN = "column";
    /**
     * where
     */
    public static final String WHERE = "where";
    /**
     * 同步开始时间
     */
    public static final String START_TIME = "startTime";
    /**
     * 同步结束时间
     */
    public static final String END_TIME = "endTime";
    /**
     * 条件时间列名
     */
    public static final String TIME_FIELD_NAME = "timeFieldName";
    /**
     * 任务数
     */
    public static final String TASK_NUM = "taskNum";
    /**
     * 神策相关配置
     */
    public static final String SA = "sa";
    /**
     * hive连接url
     */
    public static final String SA_HIVE_URL = "hiveUrl";
    /**
     * hive查询表
     */
    public static final String SA_TABLE = "table";
    /**
     * 神策类型
     */
    public static final String SA_TYPE = "type";
    /**
     * 任务切分后的开始时间
     */
    public static final String TASK_START_TIME = "taskStartTime";
    /**
     * 任务切分后的结束时间
     */
    public static final String TASK_END_TIME = "taskEndTime";
    /**
     * 小数点
     */
    public static final String POINT = ".";
    /**
     * 开始页数
     */
    public static final String START_PAGE_NO = "startPageNo";
    /**
     * 结束页数
     */
    public static final String END_PAGE_NO = "endPageNo";
    /**
     * 每页大小
     */
    public static final String PAGE_SIZE = "pageSize";
    /**
     * 使用row_number分区
     */
    public static final String USE_ROW_NUMBER = "useRowNumber";
    /**
     * 时间格式
     */
    public static final String DATE_PATTERN = "datePattern";

    /**
     * 每个task所负责的页数
     */
    public static final String RECEIVE_PAGE_SIZE = "receivePageSize";
    /**
     * 时间分片查询sql模板
     */
    public static final String SQL_TEMPLATE = "sqlTemplate";
    /**
     * rowNum函数分片查询sql模板
     */
    public static final String ROW_NUM_SQL_TEMPLATE = "rowNumSqlTemplate";
    /**
     * 按时间分区时，数据量过大时指定的每次查多久的，毫秒值
     */
    public static final String TIME_INTERVAL = "timeInterval";
}
