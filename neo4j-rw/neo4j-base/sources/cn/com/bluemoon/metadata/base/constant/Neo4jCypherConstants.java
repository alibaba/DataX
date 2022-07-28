package cn.com.bluemoon.metadata.base.constant;

import cn.com.bluemoon.metadata.base.util.ReflectionUtils;
import com.google.common.collect.Lists;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* loaded from: neo4j-base-1.0-SNAPSHOT.jar:cn/com/bluemoon/metadata/base/constant/Neo4jCypherConstants.class */
public class Neo4jCypherConstants {
    public static final String CREATE_TYPE_ON_NODE = "NODE";
    public static final String CREATE_TYPE_ON_REL = "RELATIONSHIP";
    public static final String DEFAULT_BATCH_KEY = "batches";
    public static final String CATA_LOG_NAME = "数据字典";
    public static final String MODEL_FOLDER_NAME = "模型";
    public static final String STANDARD_FOLDER_NAME = "标准";
    public static final String PG_CATA_LOG_NAME = "PG函数";
    public static final String PLAT_MENU_NAME = "平台菜单";
    public static final String ETL_FOLDER_NAME = "ETL";
    public static final String HDFS = "HDFS";
    public static final String HBASE = "HBase";
    public static final String PHOENIX = "PHOENIX";
    public static final String PHOENIX_NODE_SCHEMA_NAME = "NONE";
    public static final String RT_FOLDER_NAME = "实时";
    public static final String RT_CANAL_FOLDER_NAME = "Canal";
    public static final String RT_SOURCE_FOLDER_NAME = "Source";
    public static final String RT_UDF_FOLDER_NAME = "Udf";
    public static final String RT_BUSINESS_FOLDER_NAME = "Business";
    public static final String REPORT_NAME = "报表";
    public static final String TEMPLATE_DIRECTORY_NAME = "模板目录";
    public static final String DATA_SET_DIRECTORY_NAME = "数据集目录";
    public static final String BI_REPORT_NAME = "BI报表";
    public static final String DASHBOARD_DIRECTORY_NAME = "仪表板";
    public static final String DATA_PREPARE_NAME = "数据准备";
    public static final String GUID_DELIMITER = ";";
    public static final String PATH_DELIMITER = "/";
    public static final char CSV_DELIMITER = ',';
    public static final String META_DATA_VIRTUAL_COLUMN = "meta_data_virtual_column";
    public static final Logger log = LoggerFactory.getLogger(Neo4jCypherConstants.class);
    public static final List<String> BASE_NODE_COLUMNS = Lists.newArrayList();
    public static final List<String> BASE_RELATION_COLUMNS = Lists.newArrayList();

    static {
        Class<?> aClass = null;
        Class<?> bClass = null;
        try {
            aClass = Class.forName("cn.com.bluemoon.metadata.neo4j.dal.neo4j.base.Neo4jNodeBaseEntity");
            bClass = Class.forName("cn.com.bluemoon.metadata.neo4j.dal.neo4j.base.Neo4jRelationshipsBaseEntity");
        } catch (ClassNotFoundException e) {
            log.error("获取到基础类的属性失败！");
        }
        BASE_NODE_COLUMNS.addAll((Collection) ReflectionUtils.getFields(aClass).stream().map(x -> {
            return x.getName();
        }).collect(Collectors.toList()));
        BASE_RELATION_COLUMNS.addAll((Collection) ReflectionUtils.getFields(bClass).stream().map(x -> {
            return x.getName();
        }).collect(Collectors.toList()));
    }
}
