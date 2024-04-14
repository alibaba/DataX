package com.alibaba.datax.plugin.reader.hdfsreader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.unstructuredstorage.reader.ColumnEntry;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderErrorCode;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.RCFileRecordReader;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Created by mingya.wmy on 2015/8/12.
 */
public class DFSUtil {
    private static final Logger LOG = LoggerFactory.getLogger(HdfsReader.Job.class);

    private org.apache.hadoop.conf.Configuration hadoopConf = null;
    private String specifiedFileType = null;
    private Boolean haveKerberos = false;
    private String kerberosKeytabFilePath;
    private String kerberosPrincipal;


    private static final int DIRECTORY_SIZE_GUESS = 16 * 1024;

    public static final String HDFS_DEFAULTFS_KEY = "fs.defaultFS";
    public static final String HADOOP_SECURITY_AUTHENTICATION_KEY = "hadoop.security.authentication";

    private Boolean skipEmptyOrcFile = false;

    private Integer orcFileEmptySize = null;


    public DFSUtil(Configuration taskConfig) {
        hadoopConf = new org.apache.hadoop.conf.Configuration();
        //io.file.buffer.size 性能参数
        //http://blog.csdn.net/yangjl38/article/details/7583374
        Configuration hadoopSiteParams = taskConfig.getConfiguration(Key.HADOOP_CONFIG);
        JSONObject hadoopSiteParamsAsJsonObject = JSON.parseObject(taskConfig.getString(Key.HADOOP_CONFIG));
        if (null != hadoopSiteParams) {
            Set<String> paramKeys = hadoopSiteParams.getKeys();
            for (String each : paramKeys) {
                hadoopConf.set(each, hadoopSiteParamsAsJsonObject.getString(each));
            }
        }
        hadoopConf.set(HDFS_DEFAULTFS_KEY, taskConfig.getString(Key.DEFAULT_FS));

        //是否有Kerberos认证
        this.haveKerberos = taskConfig.getBool(Key.HAVE_KERBEROS, false);
        if (haveKerberos) {
            this.kerberosKeytabFilePath = taskConfig.getString(Key.KERBEROS_KEYTAB_FILE_PATH);
            this.kerberosPrincipal = taskConfig.getString(Key.KERBEROS_PRINCIPAL);
            this.hadoopConf.set(HADOOP_SECURITY_AUTHENTICATION_KEY, "kerberos");
        }
        this.kerberosAuthentication(this.kerberosPrincipal, this.kerberosKeytabFilePath);
        this.skipEmptyOrcFile = taskConfig.getBool(Key.SKIP_EMPTY_ORCFILE, false);

        LOG.info(String.format("hadoopConfig details:%s", JSON.toJSONString(this.hadoopConf)));
    }

    private void kerberosAuthentication(String kerberosPrincipal, String kerberosKeytabFilePath) {
        if (haveKerberos && StringUtils.isNotBlank(this.kerberosPrincipal) && StringUtils.isNotBlank(this.kerberosKeytabFilePath)) {
            UserGroupInformation.setConfiguration(this.hadoopConf);
            try {
                UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, kerberosKeytabFilePath);
            } catch (Exception e) {
                String message = String.format("kerberos认证失败,请确定kerberosKeytabFilePath[%s]和kerberosPrincipal[%s]填写正确",
                        kerberosKeytabFilePath, kerberosPrincipal);
                throw DataXException.asDataXException(HdfsReaderErrorCode.KERBEROS_LOGIN_ERROR, message, e);
            }
        }
    }

    /**
     * 获取指定路径列表下符合条件的所有文件的绝对路径
     *
     * @param srcPaths          路径列表
     * @param specifiedFileType 指定文件类型
     */
    public HashSet<String> getAllFiles(List<String> srcPaths, String specifiedFileType, Boolean skipEmptyOrcFile, Integer orcFileEmptySize) {

        this.specifiedFileType = specifiedFileType;
        this.skipEmptyOrcFile = skipEmptyOrcFile;
        this.orcFileEmptySize = orcFileEmptySize;
        if (!srcPaths.isEmpty()) {
            for (String eachPath : srcPaths) {
                LOG.info(String.format("get HDFS all files in path = [%s]", eachPath));
                getHDFSAllFiles(eachPath);
            }
        }
        return sourceHDFSAllFilesList;
    }

    private HashSet<String> sourceHDFSAllFilesList = new HashSet<String>();

    public HashSet<String> getHDFSAllFiles(String hdfsPath) {

        try {
            FileSystem hdfs = FileSystem.get(hadoopConf);
            //判断hdfsPath是否包含正则符号
            if (hdfsPath.contains("*") || hdfsPath.contains("?")) {
                Path path = new Path(hdfsPath);
                FileStatus stats[] = hdfs.globStatus(path);
                for (FileStatus f : stats) {
                    if (f.isFile()) {
                        long fileLength = f.getLen();
                        if (fileLength == 0) {
                            String message = String.format("文件[%s]长度为0，将会跳过不作处理！", hdfsPath);
                            LOG.warn(message);
                        } else if (BooleanUtils.isTrue(this.skipEmptyOrcFile) && this.orcFileEmptySize != null && fileLength <= this.orcFileEmptySize) {
                            String message = String.format("The orc file [%s] is empty, file size: %s, DataX will skip it !", f.getPath().toString(), fileLength);
                            LOG.warn(message);
                        } else {
                            addSourceFileByType(f.getPath().toString());
                        }
                    } else if (f.isDirectory()) {
                        getHDFSAllFilesNORegex(f.getPath().toString(), hdfs);
                    }
                }
            } else {
                getHDFSAllFilesNORegex(hdfsPath, hdfs);
            }

            return sourceHDFSAllFilesList;

        } catch (IOException e) {
            String message = String.format("无法读取路径[%s]下的所有文件,请确认您的配置项fs.defaultFS, path的值是否正确，" +
                    "是否有读写权限，网络是否已断开！", hdfsPath);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsReaderErrorCode.PATH_CONFIG_ERROR, e);
        }
    }

    private HashSet<String> getHDFSAllFilesNORegex(String path, FileSystem hdfs) throws IOException {

        // 获取要读取的文件的根目录
        Path listFiles = new Path(path);

        // If the network disconnected, this method will retry 45 times
        // each time the retry interval for 20 seconds
        // 获取要读取的文件的根目录的所有二级子文件目录
        FileStatus stats[] = hdfs.listStatus(listFiles);

        for (FileStatus f : stats) {
            // 判断是不是目录，如果是目录，递归调用
            if (f.isDirectory()) {
                LOG.info(String.format("[%s] 是目录, 递归获取该目录下的文件", f.getPath().toString()));
                getHDFSAllFilesNORegex(f.getPath().toString(), hdfs);
            } else if (f.isFile()) {
                long fileLength = f.getLen();
                if (fileLength == 0) {
                    String message = String.format("The file [%s] is empty, DataX will skip it !", f.getPath().toString());
                    LOG.warn(message);
                    continue;
                } else if (BooleanUtils.isTrue(this.skipEmptyOrcFile) && this.orcFileEmptySize != null && fileLength <= this.orcFileEmptySize) {
                    String message = String.format("The orc file [%s] is empty, file size: %s, DataX will skip it !", f.getPath().toString(), fileLength);
                    LOG.warn(message);
                    continue;
                }
                addSourceFileByType(f.getPath().toString());
            } else {
                String message = String.format("该路径[%s]文件类型既不是目录也不是文件，插件自动忽略。",
                        f.getPath().toString());
                LOG.info(message);
            }
        }
        return sourceHDFSAllFilesList;
    }

    // 根据用户指定的文件类型，将指定的文件类型的路径加入sourceHDFSAllFilesList
    private void addSourceFileByType(String filePath) {
        // 检查file的类型和用户配置的fileType类型是否一致
        boolean isMatchedFileType = checkHdfsFileType(filePath, this.specifiedFileType);

        if (isMatchedFileType) {
            LOG.info(String.format("[%s]是[%s]类型的文件, 将该文件加入source files列表", filePath, this.specifiedFileType));
            sourceHDFSAllFilesList.add(filePath);
        } else {
            String message = String.format("文件[%s]的类型与用户配置的fileType类型不一致，" +
                            "请确认您配置的目录下面所有文件的类型均为[%s]"
                    , filePath, this.specifiedFileType);
            LOG.error(message);
            throw DataXException.asDataXException(
                    HdfsReaderErrorCode.FILE_TYPE_UNSUPPORT, message);
        }
    }

    public InputStream getInputStream(String filepath) {
        InputStream inputStream;
        Path path = new Path(filepath);
        try {
            FileSystem fs = FileSystem.get(hadoopConf);
            //If the network disconnected, this method will retry 45 times
            //each time the retry interval for 20 seconds
            inputStream = fs.open(path);
            return inputStream;
        } catch (IOException e) {
            String message = String.format("读取文件 : [%s] 时出错,请确认文件：[%s]存在且配置的用户有权限读取", filepath, filepath);
            throw DataXException.asDataXException(HdfsReaderErrorCode.READ_FILE_ERROR, message, e);
        }
    }

    public void sequenceFileStartRead(String sourceSequenceFilePath, Configuration readerSliceConfig,
                                      RecordSender recordSender, TaskPluginCollector taskPluginCollector) {
        LOG.info(String.format("Start Read sequence file [%s].", sourceSequenceFilePath));

        Path seqFilePath = new Path(sourceSequenceFilePath);
        SequenceFile.Reader reader = null;
        try {
            //获取SequenceFile.Reader实例
            reader = new SequenceFile.Reader(this.hadoopConf,
                    SequenceFile.Reader.file(seqFilePath));
            //获取key 与 value
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), this.hadoopConf);
            Text value = new Text();
            while (reader.next(key, value)) {
                if (StringUtils.isNotBlank(value.toString())) {
                    UnstructuredStorageReaderUtil.transportOneRecord(recordSender,
                            readerSliceConfig, taskPluginCollector, value.toString());
                }
            }
        } catch (Exception e) {
            String message = String.format("SequenceFile.Reader读取文件[%s]时出错", sourceSequenceFilePath);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsReaderErrorCode.READ_SEQUENCEFILE_ERROR, message, e);
        } finally {
            IOUtils.closeStream(reader);
            LOG.info("Finally, Close stream SequenceFile.Reader.");
        }

    }

    public void rcFileStartRead(String sourceRcFilePath, Configuration readerSliceConfig,
                                RecordSender recordSender, TaskPluginCollector taskPluginCollector) {
        LOG.info(String.format("Start Read rcfile [%s].", sourceRcFilePath));
        List<ColumnEntry> column = UnstructuredStorageReaderUtil
                .getListColumnEntry(readerSliceConfig, com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COLUMN);
        // warn: no default value '\N'
        String nullFormat = readerSliceConfig.getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.NULL_FORMAT);

        Path rcFilePath = new Path(sourceRcFilePath);
        FileSystem fs = null;
        RCFileRecordReader recordReader = null;
        try {
            fs = FileSystem.get(rcFilePath.toUri(), hadoopConf);
            long fileLen = fs.getFileStatus(rcFilePath).getLen();
            FileSplit split = new FileSplit(rcFilePath, 0, fileLen, (String[]) null);
            recordReader = new RCFileRecordReader(hadoopConf, split);
            LongWritable key = new LongWritable();
            BytesRefArrayWritable value = new BytesRefArrayWritable();
            Text txt = new Text();
            while (recordReader.next(key, value)) {
                String[] sourceLine = new String[value.size()];
                txt.clear();
                for (int i = 0; i < value.size(); i++) {
                    BytesRefWritable v = value.get(i);
                    txt.set(v.getData(), v.getStart(), v.getLength());
                    sourceLine[i] = txt.toString();
                }
                UnstructuredStorageReaderUtil.transportOneRecord(recordSender,
                        column, sourceLine, nullFormat, taskPluginCollector);
            }

        } catch (IOException e) {
            String message = String.format("读取文件[%s]时出错", sourceRcFilePath);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsReaderErrorCode.READ_RCFILE_ERROR, message, e);
        } finally {
            try {
                if (recordReader != null) {
                    recordReader.close();
                    LOG.info("Finally, Close RCFileRecordReader.");
                }
            } catch (IOException e) {
                LOG.warn(String.format("finally: 关闭RCFileRecordReader失败, %s", e.getMessage()));
            }
        }

    }

    public void orcFileStartRead(String sourceOrcFilePath, Configuration readerSliceConfig,
                                 RecordSender recordSender, TaskPluginCollector taskPluginCollector) {
        LOG.info(String.format("Start Read orcfile [%s].", sourceOrcFilePath));
        List<ColumnEntry> column = UnstructuredStorageReaderUtil
                .getListColumnEntry(readerSliceConfig, com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COLUMN);
        String nullFormat = readerSliceConfig.getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.NULL_FORMAT);
        StringBuilder allColumns = new StringBuilder();
        StringBuilder allColumnTypes = new StringBuilder();
        boolean isReadAllColumns = false;
        int columnIndexMax = -1;
        // 判断是否读取所有列
        if (null == column || column.size() == 0) {
            int allColumnsCount = getAllColumnsCount(sourceOrcFilePath);
            columnIndexMax = allColumnsCount - 1;
            isReadAllColumns = true;
        } else {
            columnIndexMax = getMaxIndex(column);
        }
        for (int i = 0; i <= columnIndexMax; i++) {
            allColumns.append("col");
            allColumnTypes.append("string");
            if (i != columnIndexMax) {
                allColumns.append(",");
                allColumnTypes.append(":");
            }
        }
        if (columnIndexMax >= 0) {
            JobConf conf = new JobConf(hadoopConf);
            Path orcFilePath = new Path(sourceOrcFilePath);
            Properties p = new Properties();
            p.setProperty("columns", allColumns.toString());
            p.setProperty("columns.types", allColumnTypes.toString());
            try {
                OrcSerde serde = new OrcSerde();
                serde.initialize(conf, p);
                StructObjectInspector inspector = (StructObjectInspector) serde.getObjectInspector();
                InputFormat<?, ?> in = new OrcInputFormat();
                FileInputFormat.setInputPaths(conf, orcFilePath.toString());

                //If the network disconnected, will retry 45 times, each time the retry interval for 20 seconds
                //Each file as a split
                //TODO multy threads
                // OrcInputFormat getSplits params numSplits not used, splits size = block numbers
                InputSplit[] splits;
                try {
                    splits = in.getSplits(conf, 1);
                } catch (Exception splitException) {
                    if (Boolean.TRUE.equals(this.skipEmptyOrcFile)) {
                        boolean isOrcFileEmptyException = checkIsOrcEmptyFileExecption(splitException);
                        if (isOrcFileEmptyException) {
                            LOG.info("skipEmptyOrcFile: true, \"{}\" is an empty orc file, skip it!", sourceOrcFilePath);
                            return;
                        }
                    }
                    throw splitException;
                }
                for (InputSplit split : splits) {
                    {
                        RecordReader reader = in.getRecordReader(split, conf, Reporter.NULL);
                        Object key = reader.createKey();
                        Object value = reader.createValue();
                        // 获取列信息
                        List<? extends StructField> fields = inspector.getAllStructFieldRefs();

                        List<Object> recordFields;
                        while (reader.next(key, value)) {
                            recordFields = new ArrayList<Object>();

                            for (int i = 0; i <= columnIndexMax; i++) {
                                Object field = inspector.getStructFieldData(value, fields.get(i));
                                recordFields.add(field);
                            }
                            List<ColumnEntry> hivePartitionColumnEntrys = UnstructuredStorageReaderUtil.getListColumnEntry(readerSliceConfig, com.alibaba.datax.plugin.unstructuredstorage.reader.Key.HIVE_PARTION_COLUMN);
                            ArrayList<Column> hivePartitionColumns = new ArrayList<>();
                            hivePartitionColumns = UnstructuredStorageReaderUtil.getHivePartitionColumns(sourceOrcFilePath, hivePartitionColumnEntrys);
                            transportOneRecord(column, recordFields, recordSender,
                                    taskPluginCollector, isReadAllColumns, nullFormat,hivePartitionColumns);
                        }
                        reader.close();
                    }
                }
            } catch (Exception e) {
                String message = String.format("从orcfile文件路径[%s]中读取数据发生异常，请联系系统管理员。"
                        , sourceOrcFilePath);
                LOG.error(message);
                throw DataXException.asDataXException(HdfsReaderErrorCode.READ_FILE_ERROR, message);
            }
        } else {
            String message = String.format("请确认您所读取的列配置正确！columnIndexMax 小于0,column:%s", JSON.toJSONString(column));
            throw DataXException.asDataXException(HdfsReaderErrorCode.BAD_CONFIG_VALUE, message);
        }
    }

    private boolean checkIsOrcEmptyFileExecption(Exception e) {
        if (e == null) {
            return false;
        }

        String fullStackTrace = ExceptionUtils.getStackTrace(e);
        if (fullStackTrace.contains("org.apache.orc.impl.ReaderImpl.getRawDataSizeOfColumn") && fullStackTrace.contains("Caused by: java.lang.IndexOutOfBoundsException: Index: 1, Size: 1")) {
            return true;
        }
        return false;
    }

    private Record transportOneRecord(List<ColumnEntry> columnConfigs, List<Object> recordFields
            , RecordSender recordSender, TaskPluginCollector taskPluginCollector, boolean isReadAllColumns, String nullFormat, ArrayList<Column> hiveParitionColumns) {
        Record record = recordSender.createRecord();
        Column columnGenerated;
        try {
            if (isReadAllColumns) {
                // 读取所有列，创建都为String类型的column
                for (Object recordField : recordFields) {
                    String columnValue = null;
                    if (recordField != null) {
                        columnValue = recordField.toString();
                    }
                    columnGenerated = new StringColumn(columnValue);
                    record.addColumn(columnGenerated);
                }
            } else {
                for (ColumnEntry columnConfig : columnConfigs) {
                    String columnType = columnConfig.getType();
                    Integer columnIndex = columnConfig.getIndex();
                    String columnConst = columnConfig.getValue();

                    String columnValue = null;

                    if (null != columnIndex) {
                        if (null != recordFields.get(columnIndex))
                            columnValue = recordFields.get(columnIndex).toString();
                    } else {
                        columnValue = columnConst;
                    }
                    Type type = Type.valueOf(columnType.toUpperCase());
                    // it's all ok if nullFormat is null
                    if (StringUtils.equals(columnValue, nullFormat)) {
                        columnValue = null;
                    }
                    switch (type) {
                        case STRING:
                            columnGenerated = new StringColumn(columnValue);
                            break;
                        case LONG:
                            try {
                                columnGenerated = new LongColumn(columnValue);
                            } catch (Exception e) {
                                throw new IllegalArgumentException(String.format(
                                        "类型转换错误, 无法将[%s] 转换为[%s]", columnValue,
                                        "LONG"));
                            }
                            break;
                        case DOUBLE:
                            try {
                                columnGenerated = new DoubleColumn(columnValue);
                            } catch (Exception e) {
                                throw new IllegalArgumentException(String.format(
                                        "类型转换错误, 无法将[%s] 转换为[%s]", columnValue,
                                        "DOUBLE"));
                            }
                            break;
                        case BOOLEAN:
                            try {
                                columnGenerated = new BoolColumn(columnValue);
                            } catch (Exception e) {
                                throw new IllegalArgumentException(String.format(
                                        "类型转换错误, 无法将[%s] 转换为[%s]", columnValue,
                                        "BOOLEAN"));
                            }

                            break;
                        case DATE:
                            try {
                                if (columnValue == null) {
                                    columnGenerated = new DateColumn((Date) null);
                                } else {
                                    String formatString = columnConfig.getFormat();
                                    if (StringUtils.isNotBlank(formatString)) {
                                        // 用户自己配置的格式转换
                                        SimpleDateFormat format = new SimpleDateFormat(
                                                formatString);
                                        columnGenerated = new DateColumn(
                                                format.parse(columnValue));
                                    } else {
                                        // 框架尝试转换
                                        columnGenerated = new DateColumn(
                                                new StringColumn(columnValue)
                                                        .asDate());
                                    }
                                }
                            } catch (Exception e) {
                                throw new IllegalArgumentException(String.format(
                                        "类型转换错误, 无法将[%s] 转换为[%s]", columnValue,
                                        "DATE"));
                            }
                            break;
                        default:
                            String errorMessage = String.format(
                                    "您配置的列类型暂不支持 : [%s]", columnType);
                            LOG.error(errorMessage);
                            throw DataXException
                                    .asDataXException(
                                            UnstructuredStorageReaderErrorCode.NOT_SUPPORT_TYPE,
                                            errorMessage);
                    }

                    record.addColumn(columnGenerated);
                }
            }
            recordSender.sendToWriter(record);
        } catch (IllegalArgumentException iae) {
            taskPluginCollector
                    .collectDirtyRecord(record, iae.getMessage());
        } catch (IndexOutOfBoundsException ioe) {
            taskPluginCollector
                    .collectDirtyRecord(record, ioe.getMessage());
        } catch (Exception e) {
            if (e instanceof DataXException) {
                throw (DataXException) e;
            }
            // 每一种转换失败都是脏数据处理,包括数字格式 & 日期格式
            taskPluginCollector.collectDirtyRecord(record, e.getMessage());
        }

        return record;
    }

    private int getAllColumnsCount(String filePath) {
        Path path = new Path(filePath);
        try {
            Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(hadoopConf));
            return reader.getTypes().get(0).getSubtypesCount();
        } catch (IOException e) {
            String message = "读取orcfile column列数失败，请联系系统管理员";
            throw DataXException.asDataXException(HdfsReaderErrorCode.READ_FILE_ERROR, message);
        }
    }

    private int getMaxIndex(List<ColumnEntry> columnConfigs) {
        int maxIndex = -1;
        for (ColumnEntry columnConfig : columnConfigs) {
            Integer columnIndex = columnConfig.getIndex();
            if (columnIndex != null && columnIndex < 0) {
                String message = String.format("您column中配置的index不能小于0，请修改为正确的index,column配置:%s",
                        JSON.toJSONString(columnConfigs));
                LOG.error(message);
                throw DataXException.asDataXException(HdfsReaderErrorCode.CONFIG_INVALID_EXCEPTION, message);
            } else if (columnIndex != null && columnIndex > maxIndex) {
                maxIndex = columnIndex;
            }
        }
        return maxIndex;
    }

    private enum Type {
        STRING, LONG, BOOLEAN, DOUBLE, DATE,
    }

    public boolean checkHdfsFileType(String filepath, String specifiedFileType) {

        Path file = new Path(filepath);

        try {
            FileSystem fs = FileSystem.get(hadoopConf);
            FSDataInputStream in = fs.open(file);

            if (StringUtils.equalsIgnoreCase(specifiedFileType, Constant.CSV)
                    || StringUtils.equalsIgnoreCase(specifiedFileType, Constant.TEXT)) {

                boolean isORC = isORCFile(file, fs, in);// 判断是否是 ORC File
                if (isORC) {
                    return false;
                }
                boolean isRC = isRCFile(filepath, in);// 判断是否是 RC File
                if (isRC) {
                    return false;
                }
                boolean isSEQ = isSequenceFile(filepath, in);// 判断是否是 Sequence File
                if (isSEQ) {
                    return false;
                }
                // 如果不是ORC,RC和SEQ,则默认为是TEXT或CSV类型
                return !isORC && !isRC && !isSEQ;

            } else if (StringUtils.equalsIgnoreCase(specifiedFileType, Constant.ORC)) {

                return isORCFile(file, fs, in);
            } else if (StringUtils.equalsIgnoreCase(specifiedFileType, Constant.RC)) {

                return isRCFile(filepath, in);
            } else if (StringUtils.equalsIgnoreCase(specifiedFileType, Constant.SEQ)) {

                return isSequenceFile(filepath, in);
            } else if (StringUtils.equalsIgnoreCase(specifiedFileType, Constant.PARQUET)) {
                return true;
            }
        } catch (Exception e) {
            String message = String.format("检查文件[%s]类型失败，目前支持ORC,SEQUENCE,RCFile,TEXT,CSV五种格式的文件," +
                    "请检查您文件类型和文件是否正确。", filepath);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsReaderErrorCode.READ_FILE_ERROR, message, e);
        }
        return false;
    }

    // 判断file是否是ORC File
    private boolean isORCFile(Path file, FileSystem fs, FSDataInputStream in) {
        try {
            // figure out the size of the file using the option or filesystem
            long size = fs.getFileStatus(file).getLen();

            //read last bytes into buffer to get PostScript
            int readSize = (int) Math.min(size, DIRECTORY_SIZE_GUESS);
            in.seek(size - readSize);
            ByteBuffer buffer = ByteBuffer.allocate(readSize);
            in.readFully(buffer.array(), buffer.arrayOffset() + buffer.position(),
                    buffer.remaining());

            //read the PostScript
            //get length of PostScript
            int psLen = buffer.get(readSize - 1) & 0xff;
            int len = OrcFile.MAGIC.length();
            if (psLen < len + 1) {
                return false;
            }
            int offset = buffer.arrayOffset() + buffer.position() + buffer.limit() - 1
                    - len;
            byte[] array = buffer.array();
            // now look for the magic string at the end of the postscript.
            if (Text.decode(array, offset, len).equals(OrcFile.MAGIC)) {
                return true;
            } else {
                // If it isn't there, this may be the 0.11.0 version of ORC.
                // Read the first 3 bytes of the file to check for the header
                in.seek(0);
                byte[] header = new byte[len];
                in.readFully(header, 0, len);
                // if it isn't there, this isn't an ORC file
                if (Text.decode(header, 0, len).equals(OrcFile.MAGIC)) {
                    return true;
                }
            }
        } catch (IOException e) {
            LOG.info(String.format("检查文件类型: [%s] 不是ORC File.", file.toString()));
        }
        return false;
    }

    // 判断file是否是RC file
    private boolean isRCFile(String filepath, FSDataInputStream in) {

        // The first version of RCFile used the sequence file header.
        final byte[] ORIGINAL_MAGIC = new byte[]{(byte) 'S', (byte) 'E', (byte) 'Q'};
        // The 'magic' bytes at the beginning of the RCFile
        final byte[] RC_MAGIC = new byte[]{(byte) 'R', (byte) 'C', (byte) 'F'};
        // the version that was included with the original magic, which is mapped
        // into ORIGINAL_VERSION
        final byte ORIGINAL_MAGIC_VERSION_WITH_METADATA = 6;
        // All of the versions should be place in this list.
        final int ORIGINAL_VERSION = 0;  // version with SEQ
        final int NEW_MAGIC_VERSION = 1; // version with RCF
        final int CURRENT_VERSION = NEW_MAGIC_VERSION;
        byte version;

        byte[] magic = new byte[RC_MAGIC.length];
        try {
            in.seek(0);
            in.readFully(magic);

            if (Arrays.equals(magic, ORIGINAL_MAGIC)) {
                byte vers = in.readByte();
                if (vers != ORIGINAL_MAGIC_VERSION_WITH_METADATA) {
                    return false;
                }
                version = ORIGINAL_VERSION;
            } else {
                if (!Arrays.equals(magic, RC_MAGIC)) {
                    return false;
                }

                // Set 'version'
                version = in.readByte();
                if (version > CURRENT_VERSION) {
                    return false;
                }
            }

            if (version == ORIGINAL_VERSION) {
                try {
                    Class<?> keyCls = hadoopConf.getClassByName(Text.readString(in));
                    Class<?> valCls = hadoopConf.getClassByName(Text.readString(in));
                    if (!keyCls.equals(RCFile.KeyBuffer.class)
                            || !valCls.equals(RCFile.ValueBuffer.class)) {
                        return false;
                    }
                } catch (ClassNotFoundException e) {
                    return false;
                }
            }
            boolean decompress = in.readBoolean(); // is compressed?
            if (version == ORIGINAL_VERSION) {
                // is block-compressed? it should be always false.
                boolean blkCompressed = in.readBoolean();
                if (blkCompressed) {
                    return false;
                }
            }
            return true;
        } catch (IOException e) {
            LOG.info(String.format("检查文件类型: [%s] 不是RC File.", filepath));
        }
        return false;
    }

    // 判断file是否是Sequence file
    private boolean isSequenceFile(String filepath, FSDataInputStream in) {
        byte[] SEQ_MAGIC = new byte[]{(byte) 'S', (byte) 'E', (byte) 'Q'};
        byte[] magic = new byte[SEQ_MAGIC.length];
        try {
            in.seek(0);
            in.readFully(magic);
            if (Arrays.equals(magic, SEQ_MAGIC)) {
                return true;
            } else {
                return false;
            }
        } catch (IOException e) {
            LOG.info(String.format("检查文件类型: [%s] 不是Sequence File.", filepath));
        }
        return false;
    }

    public void parquetFileStartRead(String sourceParquetFilePath, Configuration readerSliceConfig, RecordSender recordSender, TaskPluginCollector taskPluginCollector) {
        String schemaString = readerSliceConfig.getString(Key.PARQUET_SCHEMA);
        if (StringUtils.isNotBlank(schemaString)) {
            LOG.info("You config parquet schema, use it {}", schemaString);
        } else {
            schemaString = getParquetSchema(sourceParquetFilePath, hadoopConf);
            LOG.info("Parquet schema parsed from: {} , schema is {}", sourceParquetFilePath, schemaString);
            if (StringUtils.isBlank(schemaString)) {
                throw DataXException.asDataXException("ParquetSchema is required, please check your config");
            }
        }
        MessageType parquetSchema = null;
        List<org.apache.parquet.schema.Type> parquetTypes = null;
        Map<String, ParquetMeta> parquetMetaMap = null;
        int fieldCount = 0;
        try {
            parquetSchema = MessageTypeParser.parseMessageType(schemaString);
            fieldCount = parquetSchema.getFieldCount();
            parquetTypes = parquetSchema.getFields();
            parquetMetaMap = ParquetMessageHelper.parseParquetTypes(parquetTypes);
        } catch (Exception e) {
            String message = String.format("Error parsing to MessageType via Schema string [%s]", schemaString);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsReaderErrorCode.PARSE_MESSAGE_TYPE_FROM_SCHEMA_ERROR, e);
        }
        List<ColumnEntry> column = UnstructuredStorageReaderUtil.getListColumnEntry(readerSliceConfig, com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COLUMN);
        String nullFormat = readerSliceConfig.getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.NULL_FORMAT);
        boolean isUtcTimestamp = readerSliceConfig.getBool(Key.PARQUET_UTC_TIMESTAMP, false);
        boolean isReadAllColumns = (column == null || column.size() == 0) ? true : false;
        LOG.info("ReadingAllColums: " + isReadAllColumns);

        /**
         * 支持 hive 表中间加列场景
         *
         * 开关默认 false，在 hive表存在中间加列的场景打开，需要根据 name排序
         * 不默认打开的原因
         * 1、存量hdfs任务，只根据 index获取字段，无name字段配置
         * 2、中间加列场景比较少
         * 3、存量任务可能存在列错位的问题，不能随意纠正
         */
        boolean supportAddMiddleColumn = readerSliceConfig.getBool(Key.SUPPORT_ADD_MIDDLE_COLUMN, false);

        boolean printNullValueException = readerSliceConfig.getBool("printNullValueException", false);
        List<Integer> ignoreIndex = readerSliceConfig.getList("ignoreIndex", new ArrayList<Integer>(), Integer.class);

        JobConf conf = new JobConf(hadoopConf);
        ParquetReader<Group> reader = null;
        try {
            Path parquetFilePath = new Path(sourceParquetFilePath);
            GroupReadSupport readSupport = new GroupReadSupport();
            readSupport.init(conf, null, parquetSchema);
            // 这里初始化parquetReader的时候，会getFileSystem，如果是HA集群，期间会根据hadoopConfig中区加载failover类，这里初始化builder带上conf
            ParquetReader.Builder parquetReaderBuilder = ParquetReader.builder(readSupport, parquetFilePath);
            parquetReaderBuilder.withConf(hadoopConf);
            reader = parquetReaderBuilder.build();
            Group g = null;

            // 从文件名中解析分区信息
            List<ColumnEntry> hivePartitionColumnEntrys = UnstructuredStorageReaderUtil.getListColumnEntry(readerSliceConfig, com.alibaba.datax.plugin.unstructuredstorage.reader.Key.HIVE_PARTION_COLUMN);
            ArrayList<Column> hivePartitionColumns = new ArrayList<>();
            hivePartitionColumns = UnstructuredStorageReaderUtil.getHivePartitionColumns(sourceParquetFilePath, hivePartitionColumnEntrys);
            List<String> schemaFieldList = null;
            Map<Integer, String> colNameIndexMap = null;
            Map<Integer, Integer> indexMap = null;
            if (supportAddMiddleColumn) {
                boolean nonName = column.stream().anyMatch(columnEntry -> StringUtils.isEmpty(columnEntry.getName()));
                if (nonName) {
                    throw new DataXException("You configured column item without name, please correct it");
                }
                List<org.apache.parquet.schema.Type> parquetFileFields = getParquetFileFields(parquetFilePath, hadoopConf);
                schemaFieldList = parquetFileFields.stream().map(org.apache.parquet.schema.Type::getName).collect(Collectors.toList());
                colNameIndexMap = new ConcurrentHashMap<>();
                Map<Integer, String> finalColNameIndexMap = colNameIndexMap;
                column.forEach(columnEntry -> finalColNameIndexMap.put(columnEntry.getIndex(), columnEntry.getName()));
                Iterator<Map.Entry<Integer, String>> iterator = finalColNameIndexMap.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<Integer, String> next = iterator.next();
                    if (!schemaFieldList.contains(next.getValue())) {
                        finalColNameIndexMap.remove((next.getKey()));
                    }
                }
                LOG.info("SupportAddMiddleColumn is true, fields from parquet file is {}, " +
                        "colNameIndexMap is {}", JSON.toJSONString(schemaFieldList), JSON.toJSONString(colNameIndexMap));
                fieldCount = column.size();
                indexMap = new HashMap<>();
                for (int j = 0; j < fieldCount; j++) {
                    if (colNameIndexMap.containsKey(j)) {
                        int index = findIndex(schemaFieldList, findEleInMap(colNameIndexMap, j));
                        indexMap.put(j, index);
                    }
                }
            }
            while ((g = reader.read()) != null) {
                List<Object> formattedRecord = new ArrayList<Object>(fieldCount);
                try {
                    for (int j = 0; j < fieldCount; j++) {
                        Object data = null;
                        try {
                            if (null != ignoreIndex && !ignoreIndex.isEmpty() && ignoreIndex.contains(j)) {
                                data = null;
                            } else {
                                if (supportAddMiddleColumn) {
                                    if (!colNameIndexMap.containsKey(j)) {
                                        formattedRecord.add(null);
                                        continue;
                                    } else {
                                        data = DFSUtil.this.readFields(g, parquetTypes.get(indexMap.get(j)), indexMap.get(j), parquetMetaMap, isUtcTimestamp);
                                    }
                                } else {
                                    data = DFSUtil.this.readFields(g, parquetTypes.get(j), j, parquetMetaMap, isUtcTimestamp);
                                }
                            }
                        } catch (RuntimeException e) {
                            if (printNullValueException) {
                                LOG.warn(e.getMessage());
                            }
                        }
                        formattedRecord.add(data);
                    }
                    transportOneRecord(column, formattedRecord, recordSender, taskPluginCollector, isReadAllColumns, nullFormat, hivePartitionColumns);
                } catch (Exception e) {
                    throw DataXException.asDataXException(HdfsReaderErrorCode.READ_PARQUET_ERROR, e);
                }
            }
        } catch (Exception e) {
            throw DataXException.asDataXException(HdfsReaderErrorCode.READ_PARQUET_ERROR, e);
        } finally {
            org.apache.commons.io.IOUtils.closeQuietly(reader);
        }
    }

    private String findEleInMap(Map<Integer, String> map, Integer key) {
        Iterator<Map.Entry<Integer, String>> iterator = map.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Integer, String> next = iterator.next();
            if (key.equals(next.getKey())) {
                return next.getValue();
            }
        }
        return null;
    }

    private int findIndex(List<String> schemaFieldList, String colName) {
        for (int i = 0; i < schemaFieldList.size(); i++) {
            if (schemaFieldList.get(i).equals(colName)) {
                return i;
            }
        }
        return -1;
    }

    private List<org.apache.parquet.schema.Type> getParquetFileFields(Path filePath, org.apache.hadoop.conf.Configuration configuration) {
        try (org.apache.parquet.hadoop.ParquetFileReader reader = org.apache.parquet.hadoop.ParquetFileReader.open(HadoopInputFile.fromPath(filePath, configuration))) {
            org.apache.parquet.schema.MessageType schema = reader.getFooter().getFileMetaData().getSchema();
            List<org.apache.parquet.schema.Type> fields = schema.getFields();
            return fields;
        } catch (IOException e) {
            LOG.error("Fetch parquet field error", e);
            throw new DataXException(String.format("Fetch parquet field error, msg is %s", e.getMessage()));
        }
    }

    private String getParquetSchema(String sourceParquetFilePath, org.apache.hadoop.conf.Configuration hadoopConf) {
        GroupReadSupport readSupport = new GroupReadSupport();
        ParquetReader.Builder parquetReaderBuilder = ParquetReader.builder(readSupport, new Path(sourceParquetFilePath));
        ParquetReader<Group> reader = null;
        try {
            parquetReaderBuilder.withConf(hadoopConf);
            reader = parquetReaderBuilder.build();
            Group g = null;
            if ((g = reader.read()) != null) {
                return g.getType().toString();
            }
        } catch (Throwable e) {
            LOG.error("Inner error, getParquetSchema failed, message is {}", e.getMessage());
        } finally {
            org.apache.commons.io.IOUtils.closeQuietly(reader);
        }
        return null;
    }

    /**
     * parquet 相关
     */
    private static final int JULIAN_EPOCH_OFFSET_DAYS = 2440588;
    private static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);
    private static final long NANOS_PER_MILLISECOND = TimeUnit.MILLISECONDS.toNanos(1);

    private long julianDayToMillis(int julianDay) {
        return (julianDay - JULIAN_EPOCH_OFFSET_DAYS) * MILLIS_IN_DAY;
    }

    private org.apache.parquet.schema.OriginalType getOriginalType(org.apache.parquet.schema.Type type, Map<String, ParquetMeta> parquetMetaMap) {
        ParquetMeta meta = parquetMetaMap.get(type.getName());
        return meta.getOriginalType();
    }

    private org.apache.parquet.schema.PrimitiveType asPrimitiveType(org.apache.parquet.schema.Type type, Map<String, ParquetMeta> parquetMetaMap) {
        ParquetMeta meta = parquetMetaMap.get(type.getName());
        return meta.getPrimitiveType();
    }

    private Object readFields(Group g, org.apache.parquet.schema.Type type, int index, Map<String, ParquetMeta> parquetMetaMap, boolean isUtcTimestamp) {
        if (this.getOriginalType(type, parquetMetaMap) == org.apache.parquet.schema.OriginalType.MAP) {
            Group groupData = g.getGroup(index, 0);
            List<org.apache.parquet.schema.Type> parquetTypes = groupData.getType().getFields();
            JSONObject data = new JSONObject();
            for (int i = 0; i < parquetTypes.size(); i++) {
                int j = groupData.getFieldRepetitionCount(i);
                // map key value 的对数
                for (int k = 0; k < j; k++) {
                    Group groupDataK = groupData.getGroup(0, k);
                    List<org.apache.parquet.schema.Type> parquetTypesK = groupDataK.getType().getFields();
                    if (2 != parquetTypesK.size()) {
                        // warn: 不是key value成对出现
                        throw new RuntimeException(String.format("bad parquet map type: %s", groupData.getValueToString(index, 0)));
                    }
                    Object subDataKey = this.readFields(groupDataK, parquetTypesK.get(0), 0, parquetMetaMap, isUtcTimestamp);
                    Object subDataValue = this.readFields(groupDataK, parquetTypesK.get(1), 1, parquetMetaMap, isUtcTimestamp);
                    if (StringUtils.equalsIgnoreCase("key", parquetTypesK.get(0).getName())) {
                        ((JSONObject) data).put(subDataKey.toString(), subDataValue);
                    } else {
                        ((JSONObject) data).put(subDataValue.toString(), subDataKey);
                    }
                }
            }
            return data;
        } else if (this.getOriginalType(type, parquetMetaMap) == org.apache.parquet.schema.OriginalType.MAP_KEY_VALUE) {
            Group groupData = g.getGroup(index, 0);
            List<org.apache.parquet.schema.Type> parquetTypes = groupData.getType().getFields();
            JSONObject data = new JSONObject();
            for (int i = 0; i < parquetTypes.size(); i++) {
                int j = groupData.getFieldRepetitionCount(i);
                // map key value 的对数
                for (int k = 0; k < j; k++) {
                    Group groupDataK = groupData.getGroup(0, k);
                    List<org.apache.parquet.schema.Type> parquetTypesK = groupDataK.getType().getFields();
                    if (2 != parquetTypesK.size()) {
                        // warn: 不是key value成对出现
                        throw new RuntimeException(String.format("bad parquet map type: %s", groupData.getValueToString(index, 0)));
                    }
                    Object subDataKey = this.readFields(groupDataK, parquetTypesK.get(0), 0, parquetMetaMap, isUtcTimestamp);
                    Object subDataValue = this.readFields(groupDataK, parquetTypesK.get(1), 1, parquetMetaMap, isUtcTimestamp);
                    if (StringUtils.equalsIgnoreCase("key", parquetTypesK.get(0).getName())) {
                        ((JSONObject) data).put(subDataKey.toString(), subDataValue);
                    } else {
                        ((JSONObject) data).put(subDataValue.toString(), subDataKey);
                    }
                }
            }
            return data;
        } else if (this.getOriginalType(type, parquetMetaMap) == org.apache.parquet.schema.OriginalType.LIST) {
            Group groupData = g.getGroup(index, 0);
            List<org.apache.parquet.schema.Type> parquetTypes = groupData.getType().getFields();
            JSONArray data = new JSONArray();
            for (int i = 0; i < parquetTypes.size(); i++) {
                Object subData = this.readFields(groupData, parquetTypes.get(i), i, parquetMetaMap, isUtcTimestamp);
                data.add(subData);
            }
            return data;
        } else if (this.getOriginalType(type, parquetMetaMap) == org.apache.parquet.schema.OriginalType.DECIMAL) {
            Binary binaryDate = g.getBinary(index, 0);
            if (null == binaryDate) {
                return null;
            } else {
                org.apache.hadoop.hive.serde2.io.HiveDecimalWritable decimalWritable = new org.apache.hadoop.hive.serde2.io.HiveDecimalWritable(binaryDate.getBytes(), this.asPrimitiveType(type, parquetMetaMap).getDecimalMetadata().getScale());
                // g.getType().getFields().get(1).asPrimitiveType().getDecimalMetadata().getScale()
                HiveDecimal hiveDecimal = decimalWritable.getHiveDecimal();
                if (null == hiveDecimal) {
                    return null;
                } else {
                    return hiveDecimal.bigDecimalValue();
                }
                // return decimalWritable.doubleValue();
            }
        } else if (this.getOriginalType(type, parquetMetaMap) == org.apache.parquet.schema.OriginalType.DATE) {
            return java.sql.Date.valueOf(LocalDate.ofEpochDay(g.getInteger(index, 0)));
        } else if (this.getOriginalType(type, parquetMetaMap) == org.apache.parquet.schema.OriginalType.UTF8) {
            return g.getValueToString(index, 0);
        } else {
            if (type.isPrimitive()) {
                PrimitiveType.PrimitiveTypeName primitiveTypeName = this.asPrimitiveType(type, parquetMetaMap).getPrimitiveTypeName();
                if (PrimitiveType.PrimitiveTypeName.BINARY == primitiveTypeName) {
                    return g.getValueToString(index, 0);
                } else if (PrimitiveType.PrimitiveTypeName.BOOLEAN == primitiveTypeName) {
                    return g.getValueToString(index, 0);
                } else if (PrimitiveType.PrimitiveTypeName.DOUBLE == primitiveTypeName) {
                    return g.getValueToString(index, 0);
                } else if (PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY == primitiveTypeName) {
                    return g.getValueToString(index, 0);
                } else if (PrimitiveType.PrimitiveTypeName.FLOAT == primitiveTypeName) {
                    return g.getValueToString(index, 0);
                } else if (PrimitiveType.PrimitiveTypeName.INT32 == primitiveTypeName) {
                    return g.getValueToString(index, 0);
                } else if (PrimitiveType.PrimitiveTypeName.INT64 == primitiveTypeName) {
                    return g.getValueToString(index, 0);
                } else if (PrimitiveType.PrimitiveTypeName.INT96 == primitiveTypeName) {
                    Binary dataInt96 = g.getInt96(index, 0);
                    if (null == dataInt96) {
                        return null;
                    } else {
                        ByteBuffer buf = dataInt96.toByteBuffer();
                        buf.order(ByteOrder.LITTLE_ENDIAN);
                        long timeOfDayNanos = buf.getLong();
                        int julianDay = buf.getInt();
                        if (isUtcTimestamp) {
                            // UTC
                            LocalDate localDate = LocalDate.ofEpochDay(julianDay - JULIAN_EPOCH_OFFSET_DAYS);
                            LocalTime localTime = LocalTime.ofNanoOfDay(timeOfDayNanos);
                            return Timestamp.valueOf(LocalDateTime.of(localDate, localTime));
                        } else {
                            // local time
                            long mills = julianDayToMillis(julianDay) + (timeOfDayNanos / NANOS_PER_MILLISECOND);
                            Timestamp timestamp = new Timestamp(mills);
                            timestamp.setNanos((int) (timeOfDayNanos % TimeUnit.SECONDS.toNanos(1)));
                            return timestamp;
                        }
                    }
                } else {
                    return g.getValueToString(index, 0);
                }
            } else {
                return g.getValueToString(index, 0);
            }
        }
    }


}
