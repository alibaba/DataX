package com.alibaba.datax.core.transport.exchanger;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import com.alibaba.datax.core.transport.transformer.TransformerErrorCode;
import com.alibaba.datax.core.transport.transformer.TransformerExecution;
import com.alibaba.datax.core.util.container.ClassLoaderSwapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * no comments.
 * Created by liqiang on 16/3/9.
 */
public abstract class TransformerExchanger {

    private static final Logger LOG = LoggerFactory.getLogger(TransformerExchanger.class);
    protected final TaskPluginCollector pluginCollector;

    protected final int taskGroupId;
    protected final int taskId;
    protected final Communication currentCommunication;

    private long totalExaustedTime = 0;
    private long totalFilterRecords = 0;
    private long totalSuccessRecords = 0;
    private long totalFailedRecords = 0;


    private List<TransformerExecution> transformerExecs;

    private ClassLoaderSwapper classLoaderSwapper = ClassLoaderSwapper
            .newCurrentThreadClassLoaderSwapper();


    public TransformerExchanger(int taskGroupId, int taskId, Communication communication,
                                List<TransformerExecution> transformerExecs,
                                final TaskPluginCollector pluginCollector) {

        this.transformerExecs = transformerExecs;
        this.pluginCollector = pluginCollector;
        this.taskGroupId = taskGroupId;
        this.taskId = taskId;
        this.currentCommunication = communication;
    }


    public Record doTransformer(Record record) {
        if (transformerExecs == null || transformerExecs.size() == 0) {
            return record;
        }

        Record result = record;

        long diffExaustedTime = 0;
        String errorMsg = null;
        boolean failed = false;
        for (TransformerExecution transformerInfoExec : transformerExecs) {
            long startTs = System.nanoTime();

            if (transformerInfoExec.getClassLoader() != null) {
                classLoaderSwapper.setCurrentThreadClassLoader(transformerInfoExec.getClassLoader());
            }

            /**
             * 延迟检查transformer参数的有效性，直接抛出异常，不作为脏数据
             * 不需要在插件中检查参数的有效性。但参数的个数等和插件相关的参数，在插件内部检查
             */
            if (!transformerInfoExec.isChecked()) {

                if (transformerInfoExec.getColumnIndex() != null && transformerInfoExec.getColumnIndex() >= record.getColumnNumber()) {
                    throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER,
                            String.format("columnIndex[%s] out of bound[%s]. name=%s",
                                    transformerInfoExec.getColumnIndex(), record.getColumnNumber(),
                                    transformerInfoExec.getTransformerName()));
                }
                transformerInfoExec.setIsChecked(true);
            }

            try {
                result = transformerInfoExec.getTransformer().evaluate(result, transformerInfoExec.gettContext(), transformerInfoExec.getFinalParas());
            } catch (Exception e) {
                errorMsg = String.format("transformer(%s) has Exception(%s)", transformerInfoExec.getTransformerName(),
                        e.getMessage());
                failed = true;
                //LOG.error(errorMsg, e);
                // transformerInfoExec.addFailedRecords(1);
                //脏数据不再进行后续transformer处理，按脏数据处理，并过滤该record。
                break;

            } finally {
                if (transformerInfoExec.getClassLoader() != null) {
                    classLoaderSwapper.restoreCurrentThreadClassLoader();
                }
            }

            if (result == null) {
                /**
                 * 这个null不能传到writer，必须消化掉
                 */
                totalFilterRecords++;
                //transformerInfoExec.addFilterRecords(1);
                break;
            }

            long diff = System.nanoTime() - startTs;
            //transformerInfoExec.addExaustedTime(diff);
            diffExaustedTime += diff;
            //transformerInfoExec.addSuccessRecords(1);
        }

        totalExaustedTime += diffExaustedTime;

        if (failed) {
            totalFailedRecords++;
            this.pluginCollector.collectDirtyRecord(record, errorMsg);
            return null;
        } else {
            totalSuccessRecords++;
            return result;
        }
    }

    public void doStat() {

        /**
         * todo 对于多个transformer时，各个transformer的单独统计进行显示。最后再汇总整个transformer的时间消耗.
         * 暂时不统计。
         */
//        if (transformers.size() > 1) {
//            for (ransformerInfoExec transformerInfoExec : transformers) {
//                currentCommunication.setLongCounter(CommunicationTool.TRANSFORMER_NAME_PREFIX + transformerInfoExec.getTransformerName(), transformerInfoExec.getExaustedTime());
//            }
//        }
        currentCommunication.setLongCounter(CommunicationTool.TRANSFORMER_SUCCEED_RECORDS, totalSuccessRecords);
        currentCommunication.setLongCounter(CommunicationTool.TRANSFORMER_FAILED_RECORDS, totalFailedRecords);
        currentCommunication.setLongCounter(CommunicationTool.TRANSFORMER_FILTER_RECORDS, totalFilterRecords);
        currentCommunication.setLongCounter(CommunicationTool.TRANSFORMER_USED_TIME, totalExaustedTime);
    }


}
