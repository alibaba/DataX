package com.alibaba.datax.core;

import com.alibaba.datax.common.element.ColumnCast;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.spi.ErrorCode;
import com.alibaba.datax.common.statistics.PerfTrace;
import com.alibaba.datax.common.statistics.VMInfo;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.job.JobContainer;
import com.alibaba.datax.core.taskgroup.TaskGroupContainer;
import com.alibaba.datax.core.util.ConfigParser;
import com.alibaba.datax.core.util.ConfigurationValidate;
import com.alibaba.datax.core.util.ExceptionTracker;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.core.util.container.LoadUtil;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.commons.lang3.StringUtils.endsWithIgnoreCase;

/**
 * Engine是DataX入口类，该类负责初始化Job或者Task的运行容器，并运行插件的Job或者Task逻辑
 */
public class Engine {

    private static final Logger LOG = LoggerFactory.getLogger(Engine.class);

    private static String RUNTIME_MODE;

    /**
     * 真正开始执行任务的地方
     * check job model (job/task) first
     *
     * @param allConf Configuration
     */
    public void start(Configuration allConf) {

        // 绑定column转换信息
        ColumnCast.bind(allConf);

        //初始化PluginLoader，可以获取各种插件配置
        LoadUtil.bind(allConf);

        boolean isJob = !("taskGroup".equalsIgnoreCase(allConf.getString(CoreConstant.DATAX_CORE_CONTAINER_MODEL)));
        //JobContainer会在schedule后再行进行设置和调整值
        int channelNumber = 0;
        AbstractContainer container;
        long instanceId;
        int taskGroupId = -1;
        if (isJob) {
            allConf.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_MODE, RUNTIME_MODE);
            container = new JobContainer(allConf);
            instanceId = allConf.getLong(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, 0);
        } else {
            container = new TaskGroupContainer(allConf);
            instanceId = allConf.getLong(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID);
            taskGroupId = allConf.getInt(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_ID);
            channelNumber = allConf.getInt(CoreConstant.DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL);
        }

        //缺省打开perfTrace
        boolean traceEnable = allConf.getBool(CoreConstant.DATAX_CORE_CONTAINER_TRACE_ENABLE, true);
        boolean perfReportEnable = allConf.getBool(CoreConstant.DATAX_CORE_REPORT_DATAX_PERFLOG, true);

        //standalone 模式的 datax shell任务不进行汇报
        if (instanceId == -1) {
            perfReportEnable = false;
        }

        int priority = 0;
        try {
            priority = Integer.parseInt(System.getenv("SKYNET_PRIORITY"));
        } catch (NumberFormatException e) {
            LOG.warn("priority set to 0, because NumberFormatException, the value is: {}",
                    System.getProperty("PROIORY"));
        }

        Configuration jobInfoConfig = allConf.getConfiguration(CoreConstant.DATAX_JOB_JOBINFO);
        //初始化PerfTrace
        PerfTrace perfTrace = PerfTrace.getInstance(isJob, instanceId, taskGroupId, priority, traceEnable);
        perfTrace.setJobInfo(jobInfoConfig, perfReportEnable, channelNumber);
        container.start();
    }


    /**
     * 过滤job配置信息
     *
     * @param configuration Configuration
     * @return String
     */
    public static String filterJobConfiguration(final Configuration configuration) {
        Configuration jobConfWithSetting = configuration.getConfiguration("job").clone();
        Configuration jobContent = jobConfWithSetting.getConfiguration("content");
        jobConfWithSetting.set("content", filterSensitiveConfiguration(jobContent));
        return jobConfWithSetting.beautify();
    }

    /**
     * 屏蔽敏感信息
     *
     * @param conf Configuration
     * @return Configuration
     */
    public static Configuration filterSensitiveConfiguration(Configuration conf) {
        Set<String> keys = conf.getKeys();
        for (final String key : keys) {
            boolean isSensitive = endsWithIgnoreCase(key, "password") || endsWithIgnoreCase(key, "accessKey");
            if (isSensitive && conf.get(key) instanceof String) {
                conf.set(key, conf.getString(key).replaceAll(".", "*"));
            }
        }
        return conf;
    }

    /**
     * @param args String[]
     * @throws Throwable
     */
    public static void entry(final String[] args) throws Throwable {
        Options options = new Options();
        options.addOption("job", true, "Job config.");
        options.addOption("jobid", true, "Job unique id.");
        options.addOption("mode", true, "Job runtime mode.");

        BasicParser parser = new BasicParser();
        CommandLine cl = parser.parse(options, args);
        String jobPath = cl.getOptionValue("job");
        // 如果用户没有明确指定jobid, 则 datax.py 会指定 jobid 默认值为-1
        String jobIdString = cl.getOptionValue("jobid");
        RUNTIME_MODE = cl.getOptionValue("mode");
        Configuration conf = ConfigParser.parse(jobPath);
        long jobId;
        String defaultJobId = "-1";
        if (!defaultJobId.equals(jobIdString)) {
            //  如果jobId相同，会怎样？
            jobId = Long.parseLong(jobIdString);
        } else {
            // 如果用户没有指定jobId，或jobId==1，执行后面逻辑
            // only for dsc & ds & datax 3 update
            String dscJobUrlPatternStr = "/instance/(\\d{1,})/config.xml";
            String dsJobUrlPatternStr = "/inner/job/(\\d{1,})/config";
            String dsTaskGroupUrlPatternStr = "/inner/job/(\\d{1,})/taskGroup/";
            List<String> patterns = Arrays.asList(dscJobUrlPatternStr, dsJobUrlPatternStr, dsTaskGroupUrlPatternStr);
            jobId = parseJobIdFromUrl(patterns, jobPath);
        }

        boolean isStandAloneMode = "standalone".equalsIgnoreCase(RUNTIME_MODE);
        if (!isStandAloneMode && jobId == -1) {
            // 如果不是 standalone 模式，那么 jobId 一定不能为-1
            throw DataXException
                    .asDataXException(FrameworkErrorCode.CONFIG_ERROR, "非 standalone 模式必须在 URL 中提供有效的 jobId.");
        }
        conf.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, jobId);
        //打印vmInfo
        VMInfo vmInfo = VMInfo.getVmInfo();
        if (vmInfo != null) {
            LOG.info(vmInfo.toString());
        }

        LOG.info("\n" + filterJobConfiguration(conf) + "\n");
        LOG.debug(conf.toJSON());
        ConfigurationValidate.doValidate(conf);
        Engine engine = new Engine();
        engine.start(conf);
    }


    /**
     * -1 表示未能解析到 jobId
     * <p>
     * only for dsc & ds & datax 3 update
     */
    private static long parseJobIdFromUrl(List<String> patternStringList, String url) {
        long result = -1;
        for (String patternString : patternStringList) {
            result = doParseJobIdFromUrl(patternString, url);
            if (result != -1) {
                return result;
            }
        }
        return result;
    }

    private static long doParseJobIdFromUrl(String patternString, String url) {
        Pattern pattern = Pattern.compile(patternString);
        Matcher matcher = pattern.matcher(url);
        if (matcher.find()) {
            return Long.parseLong(matcher.group(1));
        }
        return -1;
    }

    public static void main(String[] args) {
        int exitCode = 0;
        try {
            Engine.entry(args);
        } catch (Throwable e) {
            exitCode = 1;
            LOG.error("\n\n经DataX智能分析,该任务最可能的错误原因是:\n {}", ExceptionTracker.trace(e));
            if (e instanceof DataXException) {
                DataXException tempException = (DataXException) e;
                ErrorCode errorCode = tempException.getErrorCode();
                if (errorCode instanceof FrameworkErrorCode) {
                    FrameworkErrorCode tempErrorCode = (FrameworkErrorCode) errorCode;
                    exitCode = tempErrorCode.toExitValue();
                }
            }
            System.exit(exitCode);
        }
        System.exit(exitCode);
    }

}