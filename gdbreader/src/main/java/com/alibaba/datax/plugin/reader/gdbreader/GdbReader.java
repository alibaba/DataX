package com.alibaba.datax.plugin.reader.gdbreader;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.gdbreader.mapping.DefaultGdbMapper;
import com.alibaba.datax.plugin.reader.gdbreader.mapping.MappingRule;
import com.alibaba.datax.plugin.reader.gdbreader.mapping.MappingRuleFactory;
import com.alibaba.datax.plugin.reader.gdbreader.model.GdbElement;
import com.alibaba.datax.plugin.reader.gdbreader.model.GdbGraph;
import com.alibaba.datax.plugin.reader.gdbreader.model.ScriptGdbGraph;
import com.alibaba.datax.plugin.reader.gdbreader.util.ConfigHelper;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

public class GdbReader extends Reader {
    private final static int DEFAULT_FETCH_BATCH_SIZE = 200;
    private static GdbGraph graph;
    private static Key.ExportType exportType;

    /**
     * Job 中的方法仅执行一次，Task 中方法会由框架启动多个 Task 线程并行执行。
     * <p/>
     * 整个 Reader 执行流程是：
     * <pre>
     * Job类init-->prepare-->split
     *
     *                            Task类init-->prepare-->startRead-->post-->destroy
     *                            Task类init-->prepare-->startRead-->post-->destroy
     *
     *                                                                             Job类post-->destroy
     * </pre>
     */
    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration jobConfig = null;

        @Override
        public void init() {
            this.jobConfig = super.getPluginJobConf();

            /**
             * 注意：此方法仅执行一次。
             * 最佳实践：通常在这里对用户的配置进行校验：是否缺失必填项？有无错误值？有没有无关配置项？...
             * 并给出清晰的报错/警告提示。校验通常建议采用静态工具类进行，以保证本类结构清晰。
             */

            ConfigHelper.assertGdbClient(jobConfig);
            ConfigHelper.assertLabels(jobConfig);
            try {
                exportType = Key.ExportType.valueOf(jobConfig.getString(Key.EXPORT_TYPE));
            } catch (NullPointerException | IllegalArgumentException e) {
                throw DataXException.asDataXException(GdbReaderErrorCode.BAD_CONFIG_VALUE, Key.EXPORT_TYPE);
            }
        }

        @Override
        public void prepare() {
            /**
             * 注意：此方法仅执行一次。
             * 最佳实践：如果 Job 中有需要进行数据同步之前的处理，可以在此处完成，如果没有必要则可以直接去掉。
             */

            try {
                graph = new ScriptGdbGraph(jobConfig, exportType);
            } catch (RuntimeException e) {
                throw DataXException.asDataXException(GdbReaderErrorCode.FAIL_CLIENT_CONNECT, e.getMessage());
            }
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            /**
             * 注意：此方法仅执行一次。
             * 最佳实践：通常采用工具静态类完成把 Job 配置切分成多个 Task 配置的工作。
             * 这里的 adviceNumber 是框架根据用户的同步速度的要求建议的切分份数，仅供参考，不是强制必须切分的份数。
             */
            List<String> labels = ConfigHelper.assertLabels(jobConfig);

            /**
             * 配置label列表为空时，尝试查询GDB中所有label，添加到读取列表
             */
            if (labels.isEmpty()) {
                try {
                    labels.addAll(graph.getLabels().keySet());
                } catch (RuntimeException ex) {
                    throw DataXException.asDataXException(GdbReaderErrorCode.FAIL_FETCH_LABELS, ex.getMessage());
                }
            }

            if (labels.isEmpty()) {
                throw DataXException.asDataXException(GdbReaderErrorCode.FAIL_FETCH_LABELS, "none labels to read");
            }

            return ConfigHelper.splitConfig(jobConfig, labels);
        }

        @Override
        public void post() {
            /**
             * 注意：此方法仅执行一次。
             * 最佳实践：如果 Job 中有需要进行数据同步之后的后续处理，可以在此处完成。
             */
        }

        @Override
        public void destroy() {
            /**
             * 注意：此方法仅执行一次。
             * 最佳实践：通常配合 Job 中的 post() 方法一起完成 Job 的资源释放。
             */
            try {
                graph.close();
            } catch (Exception ex) {
                LOG.error("Failed to close client : {}", ex);
            }
        }

    }

    public static class Task extends Reader.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);
        private static MappingRule rule;
        private Configuration taskConfig;
        private String fetchLabel = null;

        private int rangeSplitSize;
        private int fetchBatchSize;

        @Override
        public void init() {
            this.taskConfig = super.getPluginJobConf();

            /**
             * 注意：此方法每个 Task 都会执行一次。
             * 最佳实践：此处通过对 taskConfig 配置的读取，进而初始化一些资源为 startRead()做准备。
             */
            fetchLabel = taskConfig.getString(Key.LABEL);
            fetchBatchSize = taskConfig.getInt(Key.FETCH_BATCH_SIZE, DEFAULT_FETCH_BATCH_SIZE);
            rangeSplitSize = taskConfig.getInt(Key.RANGE_SPLIT_SIZE, fetchBatchSize * 10);
            rule = MappingRuleFactory.getInstance().create(taskConfig, exportType);
        }

        @Override
        public void prepare() {
            /**
             * 注意：此方法仅执行一次。
             * 最佳实践：如果 Job 中有需要进行数据同步之后的处理，可以在此处完成，如果没有必要则可以直接去掉。
             */
        }

        @Override
        public void startRead(RecordSender recordSender) {
            /**
             * 注意：此方法每个 Task 都会执行一次。
             * 最佳实践：此处适当封装确保简洁清晰完成数据读取工作。
             */

            String start = "";
            while (true) {
                List<String> ids;
                try {
                    ids = graph.fetchIds(fetchLabel, start, rangeSplitSize);
                    if (ids.isEmpty()) {
                        break;
                    }
                    start = ids.get(ids.size() - 1);
                } catch (Exception ex) {
                    throw DataXException.asDataXException(GdbReaderErrorCode.FAIL_FETCH_IDS, ex.getMessage());
                }

                // send range fetch async
                int count = ids.size();
                List<ResultSet> resultSets = new LinkedList<>();
                for (int pos = 0; pos < count; pos += fetchBatchSize) {
                    int rangeSize = Math.min(fetchBatchSize, count - pos);
                    String endId = ids.get(pos + rangeSize - 1);
                    String beginId = ids.get(pos);

                    List<String> propNames = rule.isHasProperty() ? rule.getPropertyNames() : null;
                    try {
                        resultSets.add(graph.fetchElementsAsync(fetchLabel, beginId, endId, propNames));
                    } catch (Exception ex) {
                        // just print error logs and continues
                        LOG.error("failed to request label: {}, start: {}, end: {}, e: {}", fetchLabel, beginId, endId, ex);
                    }
                }

                // get range fetch dsl results
                resultSets.forEach(results -> {
                    try {
                        List<GdbElement> elements = graph.getElement(results);
                        elements.forEach(element -> {
                            Record record = recordSender.createRecord();
                            DefaultGdbMapper.getMapper(rule).accept(element, record);
                            recordSender.sendToWriter(record);
                        });
                        recordSender.flush();
                    } catch (Exception ex) {
                        LOG.error("failed to send records e {}", ex);
                    }
                });
            }
        }

        @Override
        public void post() {
            /**
             * 注意：此方法每个 Task 都会执行一次。
             * 最佳实践：如果 Task 中有需要进行数据同步之后的后续处理，可以在此处完成。
             */
        }

        @Override
        public void destroy() {
            /**
             * 注意：此方法每个 Task 都会执行一次。
             * 最佳实践：通常配合Task 中的 post() 方法一起完成 Task 的资源释放。
             */
        }

    }

}