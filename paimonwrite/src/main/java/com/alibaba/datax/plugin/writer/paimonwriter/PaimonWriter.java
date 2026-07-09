package com.alibaba.datax.plugin.writer.paimonwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.*;
import org.apache.paimon.types.*;
import org.apache.paimon.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.alibaba.datax.plugin.writer.paimonwriter.Key.*;
import static com.alibaba.datax.plugin.writer.paimonwriter.Key.HADOOP_SECURITY_AUTHENTICATION_KEY;
import static com.alibaba.datax.plugin.writer.paimonwriter.Key.HAVE_KERBEROS;
import static com.alibaba.datax.plugin.writer.paimonwriter.Key.PAIMON_BATCH_SIZE;
import static com.alibaba.datax.plugin.writer.paimonwriter.Key.PAIMON_CATALOG_FILE;
import static com.alibaba.datax.plugin.writer.paimonwriter.Key.PAIMON_CATALOG_HIVE;
import static com.alibaba.datax.plugin.writer.paimonwriter.Key.PAIMON_CATALOG_PATH;
import static com.alibaba.datax.plugin.writer.paimonwriter.Key.PAIMON_CONFIG;
import static com.alibaba.datax.plugin.writer.paimonwriter.Key.PAIMON_HADOOP_CONF_DIR;
import static com.alibaba.datax.plugin.writer.paimonwriter.Key.PAIMON_HIVE_CONF_DIR;
import static com.alibaba.datax.plugin.writer.paimonwriter.Key.PAIMON_METASTORE_URI;
import static com.alibaba.datax.plugin.writer.paimonwriter.Key.PAIMON_PRIMARY_KEY;
import static com.alibaba.datax.plugin.writer.paimonwriter.Key.PAIMON_TABLE_BUCKET;
import static com.alibaba.datax.plugin.writer.paimonwriter.Key.PAIMON_WRITE_OPTION_BATCH_INSERT;
import static com.alibaba.datax.plugin.writer.paimonwriter.Key.PAIMON_WRITE_OPTION_STREAM_INSERT;
import static com.alibaba.datax.plugin.writer.paimonwriter.PaimonWriterErrorCode.*;
import static com.alibaba.datax.plugin.writer.paimonwriter.PaimonWriterErrorCode.PAIMON_PARAM_LOST;

public class PaimonWriter extends Writer {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonWriter.class);

    public static class Job extends Writer.Job {
        private Configuration originalConfig;

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> list = new ArrayList<>();
            for (int i = 0; i < mandatoryNumber; i++) {
                list.add(originalConfig.clone());
            }
            return list;
        }

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
        }

        @Override
        public void destroy() {

        }
    }


    public static class Task extends Writer.Task {
        private String primaryKey;
        private String partitionFields;
        private String writeOption;
        private int batchSize;
        private Configuration sliceConfig;
        private List<Configuration> columnsList;

        private String catalogPath;
        private String catalogType;
        private Catalog catalog;
        private Table table;
        private int bucket;
        private String hiveConfDir;
        private String hadoopConfDir;
        private String metastoreUri;
        private String coreSitePath;
        private String hdfsSitePath;
        private org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();

        @Override
        public void init() {
            //获取与本task相关的配置
            this.sliceConfig = super.getPluginJobConf();
            String tableName = sliceConfig.getNecessaryValue(PAIMON_TABLE_NAME, PAIMON_ERROR_TABLE);
            String dbName = sliceConfig.getNecessaryValue(PAIMON_DB_NAME, PAIMON_ERROR_DB);
            catalogPath = sliceConfig.getNecessaryValue(PAIMON_CATALOG_PATH, PAIMON_PARAM_LOST);
            catalogType = sliceConfig.getNecessaryValue(PAIMON_CATALOG_TYPE, PAIMON_PARAM_LOST);
            bucket = sliceConfig.getInt(PAIMON_TABLE_BUCKET, 2);
            batchSize = sliceConfig.getInt(PAIMON_BATCH_SIZE, 10);
            writeOption = sliceConfig.getNecessaryValue(PAIMON_WRITE_OPTION, PAIMON_PARAM_LOST);

            partitionFields = sliceConfig.getString(PAIMON_PARTITION_FIELDS);
            primaryKey = sliceConfig.getString(PAIMON_PRIMARY_KEY);
            columnsList = sliceConfig.getListConfiguration(PAIMON_COLUMN);

            Configuration hadoopSiteParams = sliceConfig.getConfiguration(HADOOP_CONFIG);
            JSONObject hadoopSiteParamsAsJsonObject = JSON.parseObject(sliceConfig.getString(HADOOP_CONFIG));
            if (null != hadoopSiteParams) {
                Set<String> paramKeys = hadoopSiteParams.getKeys();
                for (String each : paramKeys) {
                    if(each.equals("hdfsUser")) {
                        System.setProperty("HADOOP_USER_NAME", hadoopSiteParamsAsJsonObject.getString(each));
                    } else if(each.equals("coreSitePath")) {
                        coreSitePath = hadoopSiteParamsAsJsonObject.getString(each);
                    } else if(each.equals("hdfsSitePath")) {
                        hdfsSitePath = hadoopSiteParamsAsJsonObject.getString(each);
                    } else {
                        hadoopConf.set(each, hadoopSiteParamsAsJsonObject.getString(each));
                    }
                }
            }

            try {
                //是否有Kerberos认证
                Boolean haveKerberos = sliceConfig.getBool(HAVE_KERBEROS, false);
                if(haveKerberos){
                    String kerberosKeytabFilePath = sliceConfig.getString(KERBEROS_KEYTAB_FILE_PATH);
                    String kerberosPrincipal = sliceConfig.getString(KERBEROS_PRINCIPAL);
                    hadoopConf.set(HADOOP_SECURITY_AUTHENTICATION_KEY, "kerberos");
                    this.kerberosAuthentication(kerberosPrincipal, kerberosKeytabFilePath, hadoopConf);
                }

                switch (catalogType) {
                    case PAIMON_CATALOG_FILE :
                        catalog = createFilesystemCatalog();
                        break;
                    case PAIMON_CATALOG_HIVE :
                        metastoreUri = sliceConfig.getString(PAIMON_METASTORE_URI);
                        hiveConfDir = sliceConfig.getString(PAIMON_HIVE_CONF_DIR);
                        hadoopConfDir = sliceConfig.getString(PAIMON_HADOOP_CONF_DIR);
                        catalog = createHiveCatalog();
                        break;
                    default :
                        LOG.error("unsupported catalog type :{}", catalogType);
                        break;
                }

                if(!tableExists(catalog, dbName, tableName)) {
                    LOG.info("{} 表不存在，开始创建...", dbName.concat("." + tableName));
                    CreateTable(catalog, dbName, tableName, columnsList, primaryKey.split(","), partitionFields.split(","));
                }

                table = getTable(catalog, dbName, tableName);

            } catch (Exception e) {
                LOG.error(ExceptionUtils.getStackTrace(e));
            }
        }

        @Override
        public void prepare() {

        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            Record record;
            DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            DateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            WriteBuilder writeBuilder = null;
            //Write records in distributed tasks
            TableWrite write = null;
            Boolean isStream = false;
            switch (writeOption) {
                case PAIMON_WRITE_OPTION_BATCH_INSERT:
                    writeBuilder = table.newBatchWriteBuilder().withOverwrite();
                    write = writeBuilder.newWrite();
                    break;
                case PAIMON_WRITE_OPTION_STREAM_INSERT:
                    writeBuilder = table.newStreamWriteBuilder();
                    write = writeBuilder.newWrite();
                    isStream = true;
                    break;
                default:
                    LOG.error("unsupported write option type :{}", writeOption);
            }

            TableCommit commit = null;
            List<CommitMessage> messages = null;
            AtomicLong counter = new AtomicLong(0);
            long num = 0;
            long commitIdentifier = 0;

            while ((record = recordReceiver.getFromReader()) != null) {

                GenericRow row = new GenericRow(columnsList.size());
                for (int i = 0; i < columnsList.size(); i++) {
                    Configuration configuration = columnsList.get(i);
                    String columnType = configuration.getString("type");
                    Column column = record.getColumn(i);
                    Object rawData = column.getRawData();

                    if (rawData == null) {
                        row.setField(i, null);
                        continue;
                    }

                    switch (columnType) {
                        case "int":
                            row.setField(i, Integer.parseInt(rawData.toString()));
                            break;
                        case "long":
                            row.setField(i, Long.parseLong(rawData.toString()));
                            break;
                        case "float":
                            row.setField(i, Float.parseFloat(rawData.toString()));
                            break;
                        case "double":
                            row.setField(i, Double.parseDouble(rawData.toString()));
                            break;
                        case "date":
                            row.setField(i, dateFormat.format(rawData));
                            break;
                        case "datetime":
                            row.setField(i, dateTimeFormat.format(rawData));
                            break;
                        case "boolean":
                            row.setField(i, Boolean.parseBoolean(rawData.toString()));
                            break;
                        case "string":
                            if(column instanceof DateColumn) {
                                row.setField(i, BinaryString.fromString(column.asString()));
                                break;
                            }
                        default:
                            row.setField(i, BinaryString.fromString(rawData.toString()));
                    }

                }
                try {
                    write.write(row, bucket);
                    if(isStream) {
                        num = counter.incrementAndGet();
                        commitIdentifier++;
                        if(num >= batchSize) {
                            List<CommitMessage> streamMsgs = ((StreamTableWrite) write).prepareCommit(false, commitIdentifier);
                            // Collect all CommitMessages to a global node and commit
                            StreamTableCommit stc = (StreamTableCommit)writeBuilder.newCommit();
                            stc.commit(commitIdentifier, streamMsgs);
                            counter.set(0L);
                        }
                    }

                } catch (Exception e) {
                    LOG.error("write is failed!", e);
                }

            }

            try {
                flushCache(isStream, commitIdentifier, num, writeBuilder, write, messages, commit);
            } catch (Exception e) {
                //Abort unsuccessful commit to delete data files
                if(null != commit) {
                    commit.abort(messages);
                }
                LOG.error("data commit is failed!", e);
            }

        }

        public void flushCache(boolean isStream, long commitIdentifier, long num, WriteBuilder writeBuilder, TableWrite write, List<CommitMessage> messages, TableCommit commit) throws Exception {

            if (isStream && num > 0) {
                messages = ((StreamTableWrite) write).prepareCommit(false, commitIdentifier);
                // Collect all CommitMessages to a global node and commit
                StreamTableCommit stc = (StreamTableCommit)writeBuilder.newCommit();
                stc.commit(commitIdentifier, messages);
            } else {
                messages = ((BatchTableWrite)write).prepareCommit();
                //Collect all CommitMessages to a global node and commit
                commit = writeBuilder.newCommit();

                if(commit == null || messages == null) {
                    throw new RuntimeException("commit or messages info not exist");
                }
                ((BatchTableCommit) commit).commit(messages);
            }

        }

        //file system catalog
        public Catalog createFilesystemCatalog() {
            CatalogContext context = CatalogContext.create(new org.apache.paimon.fs.Path(catalogPath));
            return CatalogFactory.createCatalog(context);
        }

        //hive catalog
        public Catalog createHiveCatalog() {
            // Paimon Hive catalog relies on Hive jars
            // You should add hive classpath or hive bundled jar.
            Options options = new Options();
            CatalogContext context;
            options.set("warehouse", catalogPath);
            options.set("metastore", catalogType);
            //默认设置为外部表
            options.set("table.type", "external");

            /**
             * 1.如果metastore uri 存在，则不需要设置 hiveConfDir
             * 2.如果metastore uri 不存在，读取 hiveConfDir下的hive-site.xml也可以
             */
            if(StringUtils.isNotBlank(metastoreUri)) {
                options.set("uri", metastoreUri);
            } else if(StringUtils.isNotBlank(hiveConfDir)) {
                options.set("hive-conf-dir", hiveConfDir);
            } else {
                throw DataXException.asDataXException(PAIMON_PARAM_LOST,
                        String.format("您提供配置文件有误，[%s]和[%s]参数，至少需要配置一个，不允许为空或者留白 .", PAIMON_METASTORE_URI, PAIMON_HIVE_CONF_DIR));
            }

            /**
             * 1：通过配置hadoop-conf-dir(目录中必须包含hive-site.xml,core-site.xml文件)来创建catalog
             * 2：通过配置hadoopConf(指定：coreSitePath：/path/core-site.xml,hdfsSitePath: /path/hdfs-site.xml)的方式来创建catalog
             */
            if(StringUtils.isNotBlank(hadoopConfDir)) {
                options.set("hadoop-conf-dir", hadoopConfDir);
                context = CatalogContext.create(options);
            }else if(StringUtils.isNotBlank(coreSitePath) && StringUtils.isNotBlank(hdfsSitePath)) {
                context = CatalogContext.create(options, hadoopConf);
            } else {
                throw DataXException.asDataXException(PAIMON_PARAM_LOST,
                        String.format("您提供配置文件有误，[%s]和[%s]参数，至少需要配置一个，不允许为空或者留白 .", PAIMON_HADOOP_CONF_DIR, "hadoopConfig:coreSiteFile&&hdfsSiteFile"));
            }

            return CatalogFactory.createCatalog(context);

        }

        public void CreateTable(Catalog catalog, String dbName, String tableName, List<Configuration> cols, String[] pks, String[] partKeys) {

            Configuration paimonTableParams = sliceConfig.getConfiguration(PAIMON_CONFIG);
            JSONObject paimonParamsAsJsonObject = JSON.parseObject(sliceConfig.getString(PAIMON_CONFIG));

            Schema.Builder schemaBuilder = Schema.newBuilder();

            if (null != paimonTableParams) {
                Set<String> paramKeys = paimonTableParams.getKeys();
                for (String each : paramKeys) {
                    schemaBuilder.option(each, paimonParamsAsJsonObject.getString(each));
                }
            }

            for (Configuration columnConfig : cols) {
                String columnName = columnConfig.getString("name");
                DataType columnType = getPaimonDataType(columnConfig.getString("type"));
                schemaBuilder.column(columnName, columnType, columnName);
            }

            if(pks != null && partKeys.length > 0) {
                schemaBuilder.primaryKey(pks);
            }

            Schema schema = schemaBuilder.build();

            if(partKeys != null && partKeys.length > 0) {
                schemaBuilder.partitionKeys(partKeys);
                schema = schemaBuilder.option("metastore.partitioned-table", "true").build();
            }

            Identifier identifier = Identifier.create(dbName, tableName);
            try {
                catalog.createTable(identifier, schema, false);
            } catch (Catalog.TableAlreadyExistException e) {
                throw new RuntimeException("table not exist");
            } catch (Catalog.DatabaseNotExistException e) {
                throw new RuntimeException("database not exist");
            }

        }

        public int getMatchValue(String typeName) {

            //获取长度
            String regex = "\\((\\d+)\\)";
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(typeName);
            int res = 0;

            if (matcher.find()) {
                res = Integer.parseInt(matcher.group(1));
            } else {
                LOG.error("{}:类型错误，请检查！", typeName);
            }
            return res;
        }

        public Pair<Integer, Integer> getDecValue (String typeName) {

            String regex = "dd\\((\\d+), (\\d+)\\)";

            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(typeName.trim());
            int left = 0;
            int right = 0;

            if (matcher.find()) {
                left = Integer.parseInt(matcher.group(1));
                right = Integer.parseInt(matcher.group(2));
            } else {
                LOG.error("{}:类型错误，请检查！", typeName);
            }

            return Pair.of(left, right);

        }

        public DataType getPaimonDataType(String typeName) {

            String type = typeName.toUpperCase();
            DataType dt = DataTypes.STRING();

            if(type.equals("BINARY") && !type.contains("VARBINARY")) {
                dt = type.contains("(") ? new BinaryType(getMatchValue(type.trim())) : new BinaryType();
            } else if(type.contains("VARBINARY")) {
                dt = type.contains("(") ? new VarBinaryType(getMatchValue(type.trim())): new VarBinaryType();
            } else if(type.contains("STRING")) {
                dt = VarCharType.STRING_TYPE;
            } else if(type.contains("VARCHAR")) {
                dt = type.contains("(") ? new VarCharType(getMatchValue(type.trim())): new VarCharType();
            } else if(type.contains("CHAR")) {
                if(type.contains("NOT NULL")) {
                    dt = new CharType().copy(false);
                }else if (type.contains("(")) {
                    dt = new CharType(getMatchValue(type.trim()));
                }else {
                    dt = new CharType();
                }
            } else if(type.contains("BOOLEAN")) {
                dt = new BooleanType();
            } else if(type.contains("BYTES")) {
                dt = new VarBinaryType(VarBinaryType.MAX_LENGTH);
            } else if(type.contains("DEC")) { // 包含 DEC 和 DECIMAL
                if(type.contains(",")) {
                    dt = new DecimalType(getDecValue(type).getLeft(), getDecValue(type).getRight());
                }else if(type.contains("(")) {
                    dt = new DecimalType(getMatchValue(type.trim()));
                }else {
                    dt = new DecimalType();
                }
            } else if(type.contains("NUMERIC") || type.contains("DECIMAL")) {
                if(type.contains(",")) {
                    dt = new DecimalType(getDecValue(type).getLeft(), getDecValue(type).getRight());
                }else if(type.contains("(")) {
                    dt = new DecimalType(getMatchValue(type.trim()));
                }else {
                    dt = new DecimalType();
                }
            } else if(type.equals("INT")) {
                dt = new IntType();
            } else if(type.equals("BIGINT") || type.equals("LONG")) {
                dt = new BigIntType();
            } else if(type.equals("TINYINT")) {
                dt = new TinyIntType();
            } else if(type.equals("SMALLINT")) {
                dt = new SmallIntType();
            } else if(type.equals("INTEGER")) {
                dt = new IntType();
            } else if(type.contains("FLOAT")) {
                dt = new FloatType();
            } else if(type.contains("DOUBLE")) {
                dt = new DoubleType();
            } else if(type.contains("DATE")) {
                dt = new DateType();
            } else if(type.contains("TIME")) {
                dt = type.contains("(") ? new TimeType(getMatchValue(type.trim())): new TimeType();
            } else if(type.contains("TIMESTAMP")) {
                switch (type) {
                    case "TIMESTAMP":
                    case "TIMESTAMP WITHOUT TIME ZONE":
                        dt = new TimestampType();
                        break;
                    case "TIMESTAMP(3)":
                    case "TIMESTAMP(3) WITHOUT TIME ZONE":
                        dt = new TimestampType(3);
                        break;
                    case "TIMESTAMP WITH LOCAL TIME ZONE":
                    case "TIMESTAMP_LTZ":
                        dt = new LocalZonedTimestampType();
                        break;
                    case "TIMESTAMP(3) WITH LOCAL TIME ZONE":
                    case "TIMESTAMP_LTZ(3)":
                        dt = new LocalZonedTimestampType(3);
                        break;
                    default:
                        LOG.error("{}:类型错误，请检查！", type);
                }
            } else {
                throw new UnsupportedOperationException(
                        "Not a supported type: " + typeName);
            }

            return dt;

        }

        public Table getTable(Catalog catalog, String dbName, String tableName) {
            try {
                Identifier identifier = Identifier.create(dbName, tableName);
                return catalog.getTable(identifier);
            } catch (Catalog.TableNotExistException e) {
                throw new RuntimeException("table not exist", e);
            }
        }

        public boolean tableExists(Catalog catalog, String dbName, String tableName) {
            Identifier identifier = Identifier.create(dbName, tableName);
            boolean exists = catalog.tableExists(identifier);
            return exists;
        }

        private void kerberosAuthentication(String kerberosPrincipal, String kerberosKeytabFilePath, org.apache.hadoop.conf.Configuration hadoopConf){
            if(StringUtils.isNotBlank(kerberosPrincipal) && StringUtils.isNotBlank(kerberosKeytabFilePath)){
                UserGroupInformation.setConfiguration(hadoopConf);
                try {
                    UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, kerberosKeytabFilePath);
                } catch (Exception e) {
                    String message = String.format("kerberos认证失败,请确定kerberosKeytabFilePath[%s]和kerberosPrincipal[%s]填写正确",
                            kerberosKeytabFilePath, kerberosPrincipal);
                    LOG.error(message);
                    throw DataXException.asDataXException(KERBEROS_LOGIN_ERROR, e);
                }
            }
        }

        @Override
        public void post() {

        }

        @Override
        public void destroy() {

        }
    }

}
