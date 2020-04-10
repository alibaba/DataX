package com.alibaba.datax.plugin.writer.gdbwriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.gdbwriter.client.GdbGraphManager;
import com.alibaba.datax.plugin.writer.gdbwriter.client.GdbWriterConfig;
import com.alibaba.datax.plugin.writer.gdbwriter.mapping.DefaultGdbMapper;
import com.alibaba.datax.plugin.writer.gdbwriter.mapping.MappingRule;
import com.alibaba.datax.plugin.writer.gdbwriter.mapping.MappingRuleFactory;
import com.alibaba.datax.plugin.writer.gdbwriter.model.GdbElement;
import com.alibaba.datax.plugin.writer.gdbwriter.model.GdbGraph;
import groovy.lang.Tuple2;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class GdbWriter extends Writer {
    private static final Logger log = LoggerFactory.getLogger(GdbWriter.class);

    private static Function<Record, GdbElement> mapper = null;
    private static GdbGraph globalGraph = null;
    private static boolean session = false;

    /**
     * Job 中的方法仅执行一次，Task 中方法会由框架启动多个 Task 线程并行执行。
     * <p/>
     * 整个 Writer 执行流程是：
     *
     * <pre>
     * Job类init-->prepare-->split
     *
     *                          Task类init-->prepare-->startWrite-->post-->destroy
     *                          Task类init-->prepare-->startWrite-->post-->destroy
     *
     *                                                                            Job类post-->destroy
     * </pre>
     */
    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration jobConfig = null;

        @Override
        public void init() {
            LOG.info("GDB datax plugin writer job init begin ...");
            this.jobConfig = getPluginJobConf();
            GdbWriterConfig.of(this.jobConfig);
            LOG.info("GDB datax plugin writer job init end.");

            /**
             * 注意：此方法仅执行一次。
             * 最佳实践：通常在这里对用户的配置进行校验：是否缺失必填项？有无错误值？有没有无关配置项？...
             * 并给出清晰的报错/警告提示。校验通常建议采用静态工具类进行，以保证本类结构清晰。
             */
        }

        @Override
        public void prepare() {
            /**
             * 注意：此方法仅执行一次。
             * 最佳实践：如果 Job 中有需要进行数据同步之前的处理，可以在此处完成，如果没有必要则可以直接去掉。
             */
            super.prepare();

            final MappingRule rule = MappingRuleFactory.getInstance().createV2(this.jobConfig);

            mapper = new DefaultGdbMapper(this.jobConfig).getMapper(rule);
            session = this.jobConfig.getBool(Key.SESSION_STATE, false);

            /**
             * client connect check before task
             */
            try {
                globalGraph = GdbGraphManager.instance().getGraph(this.jobConfig, false);
            } catch (final RuntimeException e) {
                throw DataXException.asDataXException(GdbWriterErrorCode.FAIL_CLIENT_CONNECT, e.getMessage());
            }
        }

        @Override
        public List<Configuration> split(final int mandatoryNumber) {
            /**
             * 注意：此方法仅执行一次。
             * 最佳实践：通常采用工具静态类完成把 Job 配置切分成多个 Task 配置的工作。
             * 这里的 mandatoryNumber 是强制必须切分的份数。
             */
            LOG.info("split begin...");
            final List<Configuration> configurationList = new ArrayList<Configuration>();
            for (int i = 0; i < mandatoryNumber; i++) {
                configurationList.add(this.jobConfig.clone());
            }
            LOG.info("split end...");
            return configurationList;
        }

        @Override
        public void post() {
            /**
             * 注意：此方法仅执行一次。
             * 最佳实践：如果 Job 中有需要进行数据同步之后的后续处理，可以在此处完成。
             */
            globalGraph.close();
        }

        @Override
        public void destroy() {
            /**
             * 注意：此方法仅执行一次。
             * 最佳实践：通常配合 Job 中的 post() 方法一起完成 Job 的资源释放。
             */
        }

    }

    @Slf4j
    public static class Task extends Writer.Task {

        private Configuration taskConfig;

        private int failed = 0;
        private int batchRecords;
        private ExecutorService submitService = null;
        private GdbGraph graph;

        @Override
        public void init() {
            /**
             * 注意：此方法每个 Task 都会执行一次。
             * 最佳实践：此处通过对 taskConfig 配置的读取，进而初始化一些资源为 startWrite()做准备。
             */
            this.taskConfig = super.getPluginJobConf();
			this.batchRecords = this.taskConfig.getInt(Key.MAX_RECORDS_IN_BATCH, GdbWriterConfig.DEFAULT_RECORD_NUM_IN_BATCH);
			this.submitService = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>(),
                new DefaultThreadFactory("submit-dsl"));

            if (!session) {
				this.graph = globalGraph;
            } else {
                /**
                 * 分批创建session client，由于服务端groovy编译性能的限制
                 */
                try {
                    Thread.sleep((getTaskId() / 10) * 10000);
                } catch (final Exception e) {
                    // ...
                }
                this.graph = GdbGraphManager.instance().getGraph(this.taskConfig, session);
            }
        }

        @Override
        public void prepare() {
            /**
             * 注意：此方法每个 Task 都会执行一次。
             * 最佳实践：如果 Task 中有需要进行数据同步之前的处理，可以在此处完成，如果没有必要则可以直接去掉。
             */
            super.prepare();
        }

        @Override
        public void startWrite(final RecordReceiver recordReceiver) {
            /**
             * 注意：此方法每个 Task 都会执行一次。
             * 最佳实践：此处适当封装确保简洁清晰完成数据写入工作。
             */
            Record r;
            Future<Boolean> future = null;
            List<Tuple2<Record, GdbElement>> records = new ArrayList<>(this.batchRecords);

            while ((r = recordReceiver.getFromReader()) != null) {
                try {
                    records.add(new Tuple2<>(r, mapper.apply(r)));
                } catch (final Exception ex) {
                    getTaskPluginCollector().collectDirtyRecord(r, ex);
                    continue;
                }

                if (records.size() >= this.batchRecords) {
                    wait4Submit(future);

                    final List<Tuple2<Record, GdbElement>> batch = records;
                    future = this.submitService.submit(() -> batchCommitRecords(batch));
                    records = new ArrayList<>(this.batchRecords);
                }
            }

            wait4Submit(future);
            if (!records.isEmpty()) {
                final List<Tuple2<Record, GdbElement>> batch = records;
                future = this.submitService.submit(() -> batchCommitRecords(batch));
                wait4Submit(future);
            }
        }

        private void wait4Submit(final Future<Boolean> future) {
            if (future == null) {
                return;
            }

            try {
                future.get();
            } catch (final Exception e) {
                e.printStackTrace();
            }
        }

        private boolean batchCommitRecords(final List<Tuple2<Record, GdbElement>> records) {
            final TaskPluginCollector collector = getTaskPluginCollector();
            try {
                final List<Tuple2<Record, Exception>> errors = this.graph.add(records);
                errors.forEach(t -> collector.collectDirtyRecord(t.getFirst(), t.getSecond()));
				this.failed += errors.size();
            } catch (final Exception e) {
                records.forEach(t -> collector.collectDirtyRecord(t.getFirst(), e));
				this.failed += records.size();
            }

            records.clear();
            return true;
        }

        @Override
        public void post() {
            /**
             * 注意：此方法每个 Task 都会执行一次。
             * 最佳实践：如果 Task 中有需要进行数据同步之后的后续处理，可以在此处完成。
             */
            log.info("Task done, dirty record count - {}", this.failed);
        }

        @Override
        public void destroy() {
            /**
             * 注意：此方法每个 Task 都会执行一次。
             * 最佳实践：通常配合Task 中的 post() 方法一起完成 Task 的资源释放。
             */
            if (session) {
				this.graph.close();
            }
			this.submitService.shutdown();
        }

    }

}