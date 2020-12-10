package com.alibaba.datax.core.util.container;

import org.apache.commons.lang.StringUtils;

import java.io.File;

/**
 * Created by jingxing on 14-8-25.
 */
public class CoreConstant {
	// --------------------------- 全局使用的变量(最好按照逻辑顺序，调整下成员变量顺序)
	// --------------------------------

	public static final String DATAX_CORE_CONTAINER_TASKGROUP_CHANNEL = "core.container.taskGroup.channel";

	public static final String DATAX_CORE_CONTAINER_MODEL = "core.container.model";

	public static final String DATAX_CORE_CONTAINER_JOB_ID = "core.container.job.id";

	public static final String DATAX_CORE_CONTAINER_TRACE_ENABLE = "core.container.trace.enable";

	public static final String DATAX_CORE_CONTAINER_JOB_MODE = "core.container.job.mode";

	public static final String DATAX_CORE_CONTAINER_JOB_REPORTINTERVAL = "core.container.job.reportInterval";

	public static final String DATAX_CORE_CONTAINER_JOB_SLEEPINTERVAL = "core.container.job.sleepInterval";

    public static final String DATAX_CORE_CONTAINER_TASKGROUP_ID = "core.container.taskGroup.id";

	public static final String DATAX_CORE_CONTAINER_TASKGROUP_SLEEPINTERVAL = "core.container.taskGroup.sleepInterval";

	public static final String DATAX_CORE_CONTAINER_TASKGROUP_REPORTINTERVAL = "core.container.taskGroup.reportInterval";

	public static final String DATAX_CORE_CONTAINER_TASK_FAILOVER_MAXRETRYTIMES = "core.container.task.failOver.maxRetryTimes";

	public static final String DATAX_CORE_CONTAINER_TASK_FAILOVER_RETRYINTERVALINMSEC = "core.container.task.failOver.retryIntervalInMsec";

	public static final String DATAX_CORE_CONTAINER_TASK_FAILOVER_MAXWAITINMSEC = "core.container.task.failOver.maxWaitInMsec";

    public static final String DATAX_CORE_DATAXSERVER_ADDRESS = "core.dataXServer.address";

	public static final String DATAX_CORE_DSC_ADDRESS = "core.dsc.address";

    public static final String DATAX_CORE_DATAXSERVER_TIMEOUT = "core.dataXServer.timeout";

	public static final String DATAX_CORE_REPORT_DATAX_LOG = "core.dataXServer.reportDataxLog";

	public static final String DATAX_CORE_REPORT_DATAX_PERFLOG = "core.dataXServer.reportPerfLog";

    public static final String DATAX_CORE_TRANSPORT_CHANNEL_CLASS = "core.transport.channel.class";

	public static final String DATAX_CORE_TRANSPORT_CHANNEL_CAPACITY = "core.transport.channel.capacity";

	public static final String DATAX_CORE_TRANSPORT_CHANNEL_CAPACITY_BYTE = "core.transport.channel.byteCapacity";

	public static final String DATAX_CORE_TRANSPORT_CHANNEL_SPEED_BYTE = "core.transport.channel.speed.byte";

    public static final String DATAX_CORE_TRANSPORT_CHANNEL_SPEED_RECORD = "core.transport.channel.speed.record";

	public static final String DATAX_CORE_TRANSPORT_CHANNEL_FLOWCONTROLINTERVAL = "core.transport.channel.flowControlInterval";

	public static final String DATAX_CORE_TRANSPORT_EXCHANGER_BUFFERSIZE = "core.transport.exchanger.bufferSize";

    public static final String DATAX_CORE_TRANSPORT_RECORD_CLASS = "core.transport.record.class";

	public static final String DATAX_CORE_STATISTICS_COLLECTOR_PLUGIN_TASKCLASS = "core.statistics.collector.plugin.taskClass";

	public static final String DATAX_CORE_STATISTICS_COLLECTOR_PLUGIN_MAXDIRTYNUM = "core.statistics.collector.plugin.maxDirtyNumber";

	public static final String DATAX_JOB_CONTENT_READER_NAME = "job.content[0].reader.name";

	public static final String DATAX_JOB_CONTENT_READER_PARAMETER = "job.content[0].reader.parameter";

	public static final String DATAX_JOB_CONTENT_WRITER_NAME = "job.content[0].writer.name";

	public static final String DATAX_JOB_CONTENT_WRITER_PARAMETER = "job.content[0].writer.parameter";

	public static final String DATAX_JOB_JOBINFO = "job.jobInfo";

	public static final String DATAX_JOB_CONTENT = "job.content";

	public static final String DATAX_JOB_CONTENT_TRANSFORMER = "job.content[0].transformer";

    public static final String DATAX_JOB_SETTING_KEYVERSION = "job.setting.keyVersion";

	public static final String DATAX_JOB_SETTING_SPEED_BYTE = "job.setting.speed.byte";

    public static final String DATAX_JOB_SETTING_SPEED_RECORD = "job.setting.speed.record";

	public static final String DATAX_JOB_SETTING_SPEED_CHANNEL = "job.setting.speed.channel";

	public static final String DATAX_JOB_SETTING_ERRORLIMIT = "job.setting.errorLimit";

    public static final String DATAX_JOB_SETTING_ERRORLIMIT_RECORD = "job.setting.errorLimit.record";

    public static final String DATAX_JOB_SETTING_ERRORLIMIT_PERCENT = "job.setting.errorLimit.percentage";

    public static final String DATAX_JOB_SETTING_DRYRUN = "job.setting.dryRun";

    public static final String DATAX_JOB_PREHANDLER_PLUGINTYPE = "job.preHandler.pluginType";

    public static final String DATAX_JOB_PREHANDLER_PLUGINNAME = "job.preHandler.pluginName";

    public static final String DATAX_JOB_POSTHANDLER_PLUGINTYPE = "job.postHandler.pluginType";

    public static final String DATAX_JOB_POSTHANDLER_PLUGINNAME = "job.postHandler.pluginName";
    // ----------------------------- 局部使用的变量
    public static final String JOB_WRITER = "reader";

	public static final String JOB_READER = "reader";

	public static final String JOB_TRANSFORMER = "transformer";

	public static final String JOB_READER_NAME = "reader.name";

	public static final String JOB_READER_PARAMETER = "reader.parameter";

	public static final String JOB_WRITER_NAME = "writer.name";

	public static final String JOB_WRITER_PARAMETER = "writer.parameter";

	public static final String TRANSFORMER_PARAMETER_COLUMNINDEX = "parameter.columnIndex";
	public static final String TRANSFORMER_PARAMETER_PARAS = "parameter.paras";
	public static final String TRANSFORMER_PARAMETER_CONTEXT = "parameter.context";
	public static final String TRANSFORMER_PARAMETER_CODE = "parameter.code";
	public static final String TRANSFORMER_PARAMETER_EXTRAPACKAGE = "parameter.extraPackage";

    public static final String TASK_ID = "taskId";

    // ----------------------------- 安全模块变量 ------------------

    public static final String LAST_KEYVERSION = "last.keyVersion";

    public static final String LAST_PUBLICKEY = "last.publicKey";

    public static final String LAST_PRIVATEKEY = "last.privateKey";

	public static final String LAST_SERVICE_USERNAME = "last.service.username";
    
	public static final String LAST_SERVICE_PASSWORD = "last.service.password";

    public static final String CURRENT_KEYVERSION = "current.keyVersion";

    public static final String CURRENT_PUBLICKEY = "current.publicKey";

    public static final String CURRENT_PRIVATEKEY = "current.privateKey";

	public static final String CURRENT_SERVICE_USERNAME = "current.service.username";
    
	public static final String CURRENT_SERVICE_PASSWORD = "current.service.password";

	// ----------------------------- 环境变量 ---------------------------------

	public static String DATAX_HOME = System.getProperty("datax.home");

	public static String DATAX_CONF_PATH = StringUtils.join(new String[] {
			DATAX_HOME, "conf", "core.json" }, File.separator);

	public static String DATAX_CONF_LOG_PATH = StringUtils.join(new String[] {
			DATAX_HOME, "conf", "logback.xml" }, File.separator);

    public static String DATAX_SECRET_PATH = StringUtils.join(new String[] {
            DATAX_HOME, "conf", ".secret.properties" }, File.separator);

	public static String DATAX_PLUGIN_HOME = StringUtils.join(new String[] {
			DATAX_HOME, "plugin" }, File.separator);

	public static String DATAX_PLUGIN_READER_HOME = StringUtils.join(
			new String[] { DATAX_HOME, "plugin", "reader" }, File.separator);

	public static String DATAX_PLUGIN_WRITER_HOME = StringUtils.join(
			new String[] { DATAX_HOME, "plugin", "writer" }, File.separator);

	public static String DATAX_BIN_HOME = StringUtils.join(new String[] {
			DATAX_HOME, "bin" }, File.separator);

	public static String DATAX_JOB_HOME = StringUtils.join(new String[] {
			DATAX_HOME, "job" }, File.separator);

	public static String DATAX_STORAGE_TRANSFORMER_HOME = StringUtils.join(
			new String[] { DATAX_HOME, "local_storage", "transformer" }, File.separator);

	public static String DATAX_STORAGE_PLUGIN_READ_HOME = StringUtils.join(
			new String[] { DATAX_HOME, "local_storage", "plugin","reader" }, File.separator);

	public static String DATAX_STORAGE_PLUGIN_WRITER_HOME = StringUtils.join(
			new String[] { DATAX_HOME, "local_storage", "plugin","writer" }, File.separator);

}
