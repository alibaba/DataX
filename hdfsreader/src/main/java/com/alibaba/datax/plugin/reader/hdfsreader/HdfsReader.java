package com.alibaba.datax.plugin.reader.hdfsreader;

import static com.alibaba.datax.plugin.reader.hdfsreader.Constant.supportFileTypes;
import static com.alibaba.datax.plugin.reader.hdfsreader.HdfsReaderErrorCode.CONFIG_INVALID_EXCEPTION;
import static com.alibaba.datax.plugin.reader.hdfsreader.HdfsReaderErrorCode.DEFAULT_FS_NOT_FIND_ERROR;
import static com.alibaba.datax.plugin.reader.hdfsreader.HdfsReaderErrorCode.REQUIRED_VALUE;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderUtil;
import java.io.InputStream;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsReader extends Reader {

  /**
   * Job 中的方法仅执行一次，Task 中方法会由框架启动多个 Task 线程并行执行。
   * <p/>
   * 整个 Reader 执行流程是：
   * <pre>
   * Job类init-->prepare-->split
   *
   * Task类init-->prepare-->startRead-->post-->destroy
   * Task类init-->prepare-->startRead-->post-->destroy
   *
   * Job类post-->destroy
   * </pre>
   */
  public static class Job extends Reader.Job {

    private static final Logger LOG = LoggerFactory.getLogger(Job.class);

    private Configuration readerOriginConfig = null;
    private String encoding = null;
    private HashSet<String> sourceFiles;
    private String specifiedFileType = null;
    private DFSUtil dfsUtil = null;
    private List<String> path = null;

    @Override
    public void init() {
      LOG.info("init() begin...");
      this.readerOriginConfig = super.getPluginJobConf();
      this.validate();
      dfsUtil = new DFSUtil(this.readerOriginConfig);
      LOG.info("init() ok and end...");
    }

    /**
     * 1.校验json里参数是否规范+合格 <br>
     * 2.校验字段是否正确 <br>
     */
    public void validate() {
      this.readerOriginConfig.getNecessaryValue(Key.DEFAULT_FS, DEFAULT_FS_NOT_FIND_ERROR);

      // path check
      String pathInStr = this.readerOriginConfig.getNecessaryValue(Key.PATH, REQUIRED_VALUE);
      if (!pathInStr.startsWith("[") && !pathInStr.endsWith("]")) {
        //是多个路径还是一个路径
        path = Collections.singletonList(pathInStr);
      } else {
        path = this.readerOriginConfig.getList(Key.PATH, String.class);
        if (null == path || path.size() == 0) {
          throw DataXException.asDataXException(REQUIRED_VALUE, "您需要指定待读取的源目录或文件");
        }
        for (String eachPath : path) {
          if (!eachPath.startsWith("/")) {
            String message = String.format("请检查参数path:[%s],需要配置为绝对路径", eachPath);
            LOG.error(message);
            throw DataXException.asDataXException(HdfsReaderErrorCode.ILLEGAL_VALUE, message);
          }
        }
      }

      specifiedFileType = this.readerOriginConfig.getNecessaryValue(Key.FILETYPE, REQUIRED_VALUE);
      if (!supportFileTypes.contains(specifiedFileType.toUpperCase())) {
        String message = String.format("HdfsReader插件目前支持%s，%s种格式的文件,请将fileType选项的值配置为%s",
            supportFileTypes, supportFileTypes.size(), supportFileTypes);
        throw DataXException.asDataXException(HdfsReaderErrorCode.FILE_TYPE_ERROR, message);
      }

      encoding = this.readerOriginConfig
          .getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.ENCODING, "UTF-8");

      try {
        Charsets.toCharset(encoding);
      } catch (UnsupportedCharsetException uce) {
        throw DataXException.asDataXException(HdfsReaderErrorCode.ILLEGAL_VALUE,
            String.format("不支持的编码格式 : [%s]", encoding), uce);
      } catch (Exception e) {
        throw DataXException.asDataXException(
            HdfsReaderErrorCode.ILLEGAL_VALUE,
            String.format("运行配置异常 : %s", e.getMessage()), e);
      }
      //check Kerberos
      Boolean haveKerberos = this.readerOriginConfig.getBool(Key.HAVE_KERBEROS, false);
      if (haveKerberos) {
        this.readerOriginConfig
            .getNecessaryValue(Key.KERBEROS_KEYTAB_FILE_PATH, REQUIRED_VALUE);
        this.readerOriginConfig
            .getNecessaryValue(Key.KERBEROS_PRINCIPAL, REQUIRED_VALUE);
      }

      // validate the Columns
      validateColumns();

      if (this.specifiedFileType.equalsIgnoreCase(Constant.CSV)) {
        //compress校验
        UnstructuredStorageReaderUtil.validateCompress(this.readerOriginConfig);
        UnstructuredStorageReaderUtil.validateCsvReaderConfig(this.readerOriginConfig);
      }

    }

    private void validateColumns() {

      // 检测是column 是否为 ["*"] 若是则填为空
      List<Configuration> cols = this.readerOriginConfig
          .getListConfiguration(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COLUMN);
      if (null != cols && 1 == cols.size()
          && ("\"*\"".equals(cols.get(0).toString()) || "'*'".equals(cols.get(0).toString()))) {
        readerOriginConfig
            .set(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COLUMN,
                new ArrayList<String>());
      } else {
        // column: 1. index type 2.value type 3.when type is Data, may have format
        List<Configuration> columns = this.readerOriginConfig
            .getListConfiguration(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COLUMN);

        if (null == columns || columns.size() == 0) {
          throw DataXException.asDataXException(CONFIG_INVALID_EXCEPTION, "您需要指定 columns");
        }

        if (null != columns && columns.size() != 0) {
          for (Configuration colCfg : columns) {
            colCfg.getNecessaryValue(
                com.alibaba.datax.plugin.unstructuredstorage.reader.Key.TYPE, REQUIRED_VALUE);
            Integer colIndex = colCfg
                .getInt(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.INDEX);
            String colValue = colCfg
                .getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.VALUE);

            if (null == colIndex && null == colValue) {
              throw DataXException.asDataXException(
                  HdfsReaderErrorCode.NO_INDEX_VALUE, "由于您配置了type, 则至少需要配置 index 或 value");
            }

            if (null != colIndex && null != colValue) {
              throw DataXException.asDataXException(
                  HdfsReaderErrorCode.MIXED_INDEX_VALUE, "您混合配置了index, value, 每一列同时仅能选择其中一种");
            }
          }
        }
      }
    }

    /**
     * 获取所有数据
     */
    @Override
    public void prepare() {
      LOG.info("prepare(), start to getAllFiles...");
      this.sourceFiles = dfsUtil.getAllFiles(path, specifiedFileType);
      LOG.info(String.format("您即将读取的文件数为: [%s], 列表为: [%s]",
          this.sourceFiles.size(), StringUtils.join(this.sourceFiles, ",")));
    }


    /**
     * @param adviceNumber int 没有用上，使用的是cfg中配置的files个数
     * @return
     */
    @Override
    public List<Configuration> split(int adviceNumber) {

      LOG.info("split() begin...");
      List<Configuration> readerSplitConfigs = new ArrayList<>();
      // warn:每个slice拖且仅拖一个文件,
      int splitNumber = this.sourceFiles.size();
      if (0 == splitNumber) {
        throw DataXException.asDataXException(HdfsReaderErrorCode.EMPTY_DIR_EXCEPTION,
            String.format("未能找到待读取的文件,请确认您的配置项path: %s", readerOriginConfig.getString(Key.PATH)));
      }

      List<List<String>> splitFiles = splitSourceFiles(new ArrayList<>(sourceFiles), splitNumber);
      // 如果是多个文件，则 每个文件一个线程，给每个文件的cfg拷贝一个reader的cfg
      for (List<String> files : splitFiles) {
        Configuration splitConfig = this.readerOriginConfig.clone();
        splitConfig.set(Constant.SOURCE_FILES, files);
        readerSplitConfigs.add(splitConfig);
      }
      return readerSplitConfigs;
    }


    private <T> List<List<T>> splitSourceFiles(final List<T> sourceList, int adviceNumber) {
      List<List<T>> splitList = new ArrayList<>();
      int avgLen = sourceList.size() / adviceNumber;
      avgLen = avgLen == 0 ? 1 : avgLen;

      for (int begin = 0, end; begin < sourceList.size(); begin = end) {
        end = begin + avgLen;
        if (end > sourceList.size()) {
          end = sourceList.size();
        }
        splitList.add(sourceList.subList(begin, end));
      }
      return splitList;
    }


    @Override
    public void post() {

    }

    @Override
    public void destroy() {

    }

  }

  public static class Task extends Reader.Task {

    private static Logger LOG = LoggerFactory.getLogger(Reader.Task.class);
    private Configuration taskConfig;
    private List<String> sourceFiles;
    private String specifiedFileType;
    private String encoding;
    private DFSUtil dfsUtil = null;
    private int bufferSize;

    @Override
    public void init() {

      this.taskConfig = super.getPluginJobConf();
      this.sourceFiles = this.taskConfig.getList(Constant.SOURCE_FILES, String.class);
      this.specifiedFileType = this.taskConfig
          .getNecessaryValue(Key.FILETYPE, REQUIRED_VALUE);
      this.encoding = this.taskConfig
          .getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.ENCODING, "UTF-8");
      this.dfsUtil = new DFSUtil(this.taskConfig);
      this.bufferSize = this.taskConfig
          .getInt(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.BUFFER_SIZE,
              com.alibaba.datax.plugin.unstructuredstorage.reader.Constant.DEFAULT_BUFFER_SIZE);
    }

    @Override
    public void prepare() {

    }

    /**
     * 根据文件类型，选择UnstructuredStorageReaderUtil 或dfsUtil 进行read
     *
     * @param sender RecordSender
     */
    @Override
    public void startRead(RecordSender sender) {

      LOG.info("read start");
      for (String hdfsFile : this.sourceFiles) {
        LOG.info(String.format("reading file : [%s]", hdfsFile));

        TaskPluginCollector taskPluginCollector = this.getTaskPluginCollector();
        switch (specifiedFileType.toUpperCase()) {
          case Constant.TEXT:
          case Constant.CSV:
            InputStream is = dfsUtil.getInputStream(hdfsFile);
            UnstructuredStorageReaderUtil
                .readFromStream(is, hdfsFile, taskConfig, sender, taskPluginCollector);
            break;
          case Constant.ORC:
            dfsUtil.orcFileStartRead(hdfsFile, taskConfig, sender, taskPluginCollector);
            break;
          case Constant.SEQ:
            dfsUtil.sequenceFileStartRead(hdfsFile, taskConfig, sender, taskPluginCollector);
            break;
          case Constant.RC:
            dfsUtil.rcFileStartRead(hdfsFile, this.taskConfig, sender, taskPluginCollector);
            break;
          default:
            String message = "HdfsReader插件目前支持ORC, TEXT, CSV, SEQUENCE, RC五种格式的文件," +
                "请将fileType选项的值配置为ORC, TEXT, CSV, SEQUENCE 或者 RC";
            throw DataXException.asDataXException(HdfsReaderErrorCode.FILE_TYPE_UNSUPPORT, message);
        }

        if (sender != null) {
          sender.flush();
        }
      }
      LOG.info("end read source files...");
    }

    @Override
    public void post() {
    }

    @Override
    public void destroy() {
    }

  }

}