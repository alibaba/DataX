package com.alibaba.datax.plugin.writer.restwriter;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.http.HttpException;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.restwriter.conf.ClientConfig;
import com.alibaba.datax.plugin.writer.restwriter.conf.Field;
import com.alibaba.datax.plugin.writer.restwriter.conf.Process;
import com.alibaba.datax.plugin.writer.restwriter.handler.ObjectRecordConverter;
import com.alibaba.datax.plugin.writer.restwriter.handler.TypeHandlerRegistry;
import com.alibaba.datax.plugin.writer.restwriter.process.ProcessExecutor;
import com.alibaba.datax.plugin.writer.restwriter.process.ProcessFactory;
import com.alibaba.datax.plugin.writer.restwriter.validator.ConfigurationValidator;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONException;
import com.google.common.collect.Lists;

import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeExecutor;
import dev.failsafe.RateLimiter;
import dev.failsafe.RetryPolicy;
import kong.unirest.HttpMethod;
import kong.unirest.HttpResponse;
import kong.unirest.HttpStatus;
import kong.unirest.JsonNode;
import kong.unirest.Unirest;
import kong.unirest.UnirestInstance;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

import static com.alibaba.datax.plugin.writer.restwriter.Key.BATCH_MODE;
import static com.alibaba.datax.plugin.writer.restwriter.Key.BATCH_SIZE;
import static com.alibaba.datax.plugin.writer.restwriter.Key.CLIENT;
import static com.alibaba.datax.plugin.writer.restwriter.Key.DEBUG;
import static com.alibaba.datax.plugin.writer.restwriter.Key.FAIL_FAST;
import static com.alibaba.datax.plugin.writer.restwriter.Key.FIELDS;
import static com.alibaba.datax.plugin.writer.restwriter.Key.HTTP_HEADERS;
import static com.alibaba.datax.plugin.writer.restwriter.Key.HTTP_METHOD;
import static com.alibaba.datax.plugin.writer.restwriter.Key.HTTP_QUERY;
import static com.alibaba.datax.plugin.writer.restwriter.Key.HTTP_SSL;
import static com.alibaba.datax.plugin.writer.restwriter.Key.MAX_RETRIES;
import static com.alibaba.datax.plugin.writer.restwriter.Key.RATE_PER_TASK;
import static com.alibaba.datax.plugin.writer.restwriter.Key.TASK_INDEX;
import static com.alibaba.datax.plugin.writer.restwriter.Key.URL;
import static com.alibaba.datax.plugin.writer.restwriter.RestWriterErrorCode.HTTP_CLIENT_CONFIG_INVALID_EXCEPTION;
import static com.alibaba.datax.plugin.writer.restwriter.RestWriterErrorCode.RUNTIME_EXCEPTION;
import static com.alibaba.datax.plugin.writer.restwriter.process.ProcessCategory.POSTPROCESS;
import static com.alibaba.datax.plugin.writer.restwriter.process.ProcessCategory.PREPROCESS;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static kong.unirest.ContentType.APPLICATION_JSON;
import static kong.unirest.HeaderNames.CONTENT_TYPE;
import static org.apache.commons.collections4.CollectionUtils.isNotEmpty;
import static org.apache.commons.collections4.MapUtils.emptyIfNull;
import static org.apache.commons.lang3.StringUtils.prependIfMissing;

/**
 * @author zhangyongxiang
 * @date 2023-08-23
 */
@Slf4j
public class RestWriter extends Writer {
    
    public static final int DEFAULT_MAX_RETRIES_VALUE = 3;
    
    public static final int DEFAULT_BATCH_SIZE_VALUE = 100;
    
    public static final boolean DEFAULT_BATCH_MODE_VALUE = false;
    
    public static final boolean DEFAULT_DEBUG_VALUE = false;
    
    public static final boolean DEFAULT_FAIL_FAST_VALUE = false;
    
    public static final boolean DEFAULT_SSL_VALUE = false;
    
    @Slf4j
    @EqualsAndHashCode(callSuper = true)
    public static class Job extends Writer.Job {
        
        private Configuration originalConfig;
        
        private long startTime;
        
        private long endTime;
        
        private Process preprocess;
        
        private Process postprocess;
        
        private ProcessFactory processFactory;
        
        private ProcessExecutor processExecutor;
        
        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            this.validateParameter();
            this.processFactory = new ProcessFactory(this.originalConfig);
            this.preprocess = this.processFactory.createProcess(PREPROCESS);
            this.postprocess = this.processFactory.createProcess(POSTPROCESS);
            this.processExecutor = new ProcessExecutor();
            log.info(
                    "{} job initialized, desc: {}, developer: {}, job conf: {}, job preprocess: {}, job postprocess: {}",
                    this.getPluginName(), this.getDescription(),
                    this.getDeveloper(), this.getPluginJobConf(),
                    this.preprocess, this.postprocess);
        }
        
        private void validateParameter() {
            try {
                new ConfigurationValidator().validate(this.originalConfig,
                        null);
            } catch (final Exception se) {
                throw DataXException.asDataXException(RUNTIME_EXCEPTION,
                        "an exception has occurred when validating parameters",
                        se);
            }
        }
        
        @Override
        public void preCheck() {
            log.info("job {} pre check will not be called",
                    this.getPluginName());
        }
        
        @Override
        public void prepare() {
            this.startTime = System.currentTimeMillis();
            
            if (nonNull(this.preprocess)
                    && isNotEmpty(this.preprocess.getOperations())) {
                this.processExecutor.execute(this.preprocess);
                log.info(
                        "{} job prepared successfully after preprocess, job conf: {}, preprocess: {}",
                        this.getPluginName(), this.originalConfig,
                        this.preprocess);
            } else {
                log.info(
                        "{} job prepared without need any of preprocess, job conf: {}",
                        this.getPluginName(), this.originalConfig);
            }
        }
        
        @Override
        public List<Configuration> split(final int mandatoryNumber) {
            int finalMandatoryNumber = mandatoryNumber;
            if (finalMandatoryNumber < 1) {
                log.warn(
                        "mandatory number {} less than one, reset it to be one",
                        mandatoryNumber);
                finalMandatoryNumber = 1;
            }
            final List<Configuration> configurations = Lists
                    .newArrayListWithExpectedSize(finalMandatoryNumber);
            
            for (int index = 0; index < finalMandatoryNumber; index++) {
                final Configuration taskConf = this.originalConfig.clone();
                taskConf.set(TASK_INDEX, index);
                configurations.add(taskConf);
                log.info(
                        "{} job split into {} tasks, current task: {}, desc: {}, developer: {}, task conf: {}",
                        this.getPluginName(), finalMandatoryNumber, index,
                        this.getDescription(), this.getDeveloper(), taskConf);
            }
            return configurations;
        }
        
        @Override
        public void post() {
            this.endTime = System.currentTimeMillis();
            
            if (nonNull(this.postprocess)
                    && isNotEmpty(this.postprocess.getOperations())) {
                this.processExecutor.execute(this.postprocess);
                log.info(
                        "{} postprocess execute successfully,  postprocess: {}",
                        this.getPluginName(), this.postprocess);
            }
            
            log.info(
                    "job {} execute to end, start from {}, end to {}, total time: {}",
                    this.getPluginName(),
                    Instant.ofEpochMilli(this.startTime)
                            .atZone(ZoneId.systemDefault()).toLocalDateTime(),
                    Instant.ofEpochMilli(this.endTime)
                            .atZone(ZoneId.systemDefault()).toLocalDateTime(),
                    Duration.ofMillis(this.endTime - this.startTime));
        }
        
        @Override
        public void destroy() {
            log.info("job {} destroy, nothing to clean up",
                    this.getPluginName());
        }
    }
    
    @Slf4j
    @EqualsAndHashCode(callSuper = true)
    public static class Task extends Writer.Task {
        
        private long startTime;
        
        private long endTime;
        
        private int successCount;
        
        private int failCount;
        
        private UnirestInstance unirest;
        
        private ObjectRecordConverter converter;
        
        private FailsafeExecutor<HttpResponse<JsonNode>> executor;
        
        private Configuration writerSliceConfig;
        
        private Integer taskIndex;
        
        private String url;
        
        private HttpMethod method;
        
        private boolean ssl;
        
        private Map<String, String> headers;
        
        private Map<String, Object> query;
        
        private Integer maxRetries;
        
        private boolean batchMode;
        
        private Integer batchSize;
        
        private List<Field> fields;
        
        private boolean debug;
        
        private boolean failFast;
        
        private Integer ratePerTask;
        
        private Duration avgWriteTime;
        
        private final ClientConfig clientConfig = new ClientConfig();
        
        @Override
        public void init() {
            this.writerSliceConfig = this.getPluginJobConf();
            this.taskIndex = this.writerSliceConfig.getInt(TASK_INDEX);
            this.url = this.writerSliceConfig.getString(URL).trim();
            this.method = HttpMethod
                    .valueOf(this.writerSliceConfig.getString(HTTP_METHOD));
            this.ssl = this.writerSliceConfig.getBool(HTTP_SSL,
                    DEFAULT_SSL_VALUE);
            this.headers = emptyIfNull(
                    this.writerSliceConfig.getMap(HTTP_HEADERS, String.class));
            this.query = emptyIfNull(this.writerSliceConfig.getMap(HTTP_QUERY));
            this.maxRetries = this.writerSliceConfig.getInt(MAX_RETRIES,
                    DEFAULT_MAX_RETRIES_VALUE);
            this.batchMode = this.writerSliceConfig.getBool(BATCH_MODE,
                    DEFAULT_BATCH_MODE_VALUE);
            this.batchSize = this.writerSliceConfig.getInt(BATCH_SIZE,
                    DEFAULT_BATCH_SIZE_VALUE);
            this.fields = this.writerSliceConfig.getListWithJson(FIELDS,
                    Field.class);
            this.debug = this.writerSliceConfig.getBool(DEBUG,
                    DEFAULT_DEBUG_VALUE);
            this.failFast = this.writerSliceConfig.getBool(FAIL_FAST,
                    DEFAULT_FAIL_FAST_VALUE);
            this.ratePerTask = this.writerSliceConfig.getInt(RATE_PER_TASK);
            final Object client = this.writerSliceConfig.get(CLIENT);
            if (nonNull(client)) {
                try {
                    final ClientConfig config = JSON.parseObject(
                            JSON.toJSONString(client), ClientConfig.class);
                    if (nonNull(config)) {
                        if (config.getMaxPerRoute() > 0) {
                            this.clientConfig
                                    .setMaxPerRoute(config.getMaxPerRoute());
                        }
                        if (config.getMaxTotal() > 0) {
                            this.clientConfig.setMaxTotal(config.getMaxTotal());
                        }
                    }
                } catch (final JSONException e) {
                    throw DataXException.asDataXException(
                            HTTP_CLIENT_CONFIG_INVALID_EXCEPTION,
                            String.format("client config: %s",
                                    JSON.toJSONString(client)));
                }
            }
            log.info(
                    "{} task {} initialized, desc: {}, developer: {}, task conf: {}",
                    this.getPluginName(), this.taskIndex, this.getDescription(),
                    this.getDeveloper(), this.writerSliceConfig);
        }
        
        @Override
        public void prepare() {
            if (this.url.startsWith("http://")) {
                this.ssl = false;
            } else if (this.url.startsWith("https://")) {
                this.ssl = true;
            } else if (this.ssl) {
                this.url = prependIfMissing(this.url, "https://");
            } else {
                this.url = prependIfMissing(this.url, "http://");
            }
            
            this.unirest = Unirest.spawnInstance();
            if (!emptyIfNull(this.headers).isEmpty()) {
                this.headers.forEach(this.unirest.config()::addDefaultHeader);
            }
            if (!this.headers.containsKey(CONTENT_TYPE)) {
                this.unirest.config().addDefaultHeader(CONTENT_TYPE,
                        APPLICATION_JSON.getMimeType());
            }
            this.unirest.config().addShutdownHook(true);
            this.unirest.config().defaultBaseUrl(this.url);
            this.unirest.config().verifySsl(false);
            this.unirest.config().automaticRetries(false);
            this.unirest.config().concurrency(this.clientConfig.getMaxTotal(),
                    this.clientConfig.getMaxPerRoute());
            
            this.converter = new ObjectRecordConverter(
                    new TypeHandlerRegistry(), this.fields);
            
            final RetryPolicy<HttpResponse<JsonNode>> retryPolicy = RetryPolicy
                    .<HttpResponse<JsonNode>>builder()
                    .handleResultIf(response -> response
                            .getStatus() >= HttpStatus.BAD_REQUEST)
                    .onFailedAttempt(e -> log.error(
                            "write failed, attempt execution times: {},"
                                    + " possible result response code: {},  possible result response body: {}",
                            e.getAttemptCount() + 1,
                            Optional.ofNullable(e.getLastResult())
                                    .map(HttpResponse::getStatusText)
                                    .orElse(null),
                            Optional.ofNullable(e.getLastResult())
                                    .map(HttpResponse::getBody).orElse(null),
                            e.getLastException()))
                    .onRetry(e -> log.warn("failure #{}th retrying.",
                            e.getAttemptCount()))
                    .onRetriesExceeded(e -> log.error(
                            "fail to write. max retries exceeded. cause: {}",
                            nonNull(e.getException())
                                    ? e.getException().getMessage()
                                    : e.getResult().getStatusText(),
                            e.getException()))
                    .build();
            if (isNull(this.ratePerTask) || this.ratePerTask <= 0) {
                this.executor = Failsafe.with(retryPolicy);
            } else {
                this.executor = Failsafe
                        .with(RateLimiter
                                .<HttpResponse<JsonNode>>smoothBuilder(
                                        this.ratePerTask, Duration.ofSeconds(1))
                                .withMaxWaitTime(Duration.ofDays(365)).build())
                        .compose(retryPolicy);
            }
            this.startTime = System.currentTimeMillis();
            this.successCount = 0;
            this.failCount = 0;
            log.info(
                    "{} task {} prepared, desc: {}, developer: {}, task conf: {}",
                    this.getPluginName(), this.taskIndex, this.getDescription(),
                    this.getDeveloper(), this.writerSliceConfig);
            log.info("http client config: {}", this.clientConfig);
        }
        
        @Override
        public void preCheck() {
            log.info(
                    "{} task {} check will not be called, desc: {}, developer: {}, task conf {}",
                    this.getPluginName(), this.taskIndex, this.getDescription(),
                    this.getDeveloper(), this.writerSliceConfig);
        }
        
        @Override
        public void startWrite(final RecordReceiver lineReceiver) {
            final List<Record> writerBuffer = new ArrayList<>(this.batchSize);
            Record recordItem = null;
            while ((recordItem = lineReceiver.getFromReader()) != null) {
                if (this.batchMode) {
                    writerBuffer.add(recordItem);
                    if (writerBuffer.size() >= this.batchSize) {
                        this.doWrite(writerBuffer);
                        writerBuffer.clear();
                    }
                    
                } else {
                    this.doWrite(recordItem);
                }
                if (this.debug) {
                    final int bound = recordItem.getColumnNumber();
                    for (int index = 0; index < bound; index++) {
                        final Column column = recordItem.getColumn(index);
                        log.info(
                                "colum type: {}, column type class: {}, raw data: {}, raw data class: {}, byte size: {}",
                                column.getType(),
                                column.getType().getClass().getName(),
                                column.getRawData(),
                                Optional.ofNullable(column.getRawData())
                                        .map(Object::getClass)
                                        .map(Class::getName).orElse(null),
                                column.getByteSize());
                    }
                }
            }
            if (this.batchMode && !writerBuffer.isEmpty()) {
                this.doWrite(writerBuffer);
                writerBuffer.clear();
            }
            
        }
        
        @Override
        public void post() {
            this.endTime = System.currentTimeMillis();
            this.unirest.close();
            log.info(
                    "job {} task {} execute to end, start from {}, end to {}, total time: {}, avg write time: {}, "
                            + "total count: {}, fail count: {}",
                    this.getPluginName(), this.taskIndex,
                    Instant.ofEpochMilli(this.startTime)
                            .atZone(ZoneId.systemDefault()).toLocalDateTime(),
                    Instant.ofEpochMilli(this.endTime)
                            .atZone(ZoneId.systemDefault()).toLocalDateTime(),
                    Duration.ofMillis(this.endTime - this.startTime),
                    this.avgWriteTime, this.successCount + this.failCount,
                    this.failCount);
            if (this.failCount > 0) {
                log.error("job {} task {} execute to end, fail count: {}",
                        this.getPluginName(), this.taskIndex, this.failCount);
            }
        }
        
        @Override
        public void destroy() {
            
            log.info("job {} task {} destroy", this.getPluginName(),
                    this.taskIndex);
        }
        
        private void doWrite(final Record item) {
            try {
                final HttpResponse<JsonNode> resp = this.executor.get(ctx -> {
                    final Map<String, Object> body = this.converter
                            .convert(item);
                    return executeRequest(1, body);
                });
                if (resp.getStatus() >= HttpStatus.BAD_REQUEST) {
                    throw new HttpException(resp.getStatusText());
                }
            } catch (final Exception e) {
                if (this.failFast) {
                    throw DataXException.asDataXException(RUNTIME_EXCEPTION,
                            e.getMessage(), e);
                }
            }
            
        }
        
        private void doWrite(final List<Record> records) {
            try {
                final HttpResponse<JsonNode> resp = this.executor.get(ctx -> {
                    final List<Map<String, Object>> body = records.stream()
                            .map(this.converter::convert)
                            .collect(Collectors.toList());
                    return executeRequest(records.size(), body);
                });
                if (resp.getStatus() >= HttpStatus.BAD_REQUEST) {
                    throw new HttpException(resp.getStatusText());
                }
            } catch (final Exception e) {
                if (this.failFast) {
                    throw DataXException.asDataXException(RUNTIME_EXCEPTION,
                            e.getMessage(), e);
                }
            }
        }
        
        private HttpResponse<JsonNode> executeRequest(final int itemCount,
                final Object body) {
            final long writeStartTime = System.nanoTime();
            return this.unirest.request(this.method.name(), "")
                    .queryString(this.query).body(body).asJson()
                    .ifSuccess(response -> {
                        this.successCount += itemCount;
                        final long writeEndTime = System.nanoTime();
                        log.info(
                                "the {}th record has been written successfully, consume time: {}",
                                this.successCount + this.failCount,
                                Duration.ofNanos(
                                        writeEndTime - writeStartTime));
                        if (isNull(this.avgWriteTime)) {
                            this.avgWriteTime = Duration
                                    .ofNanos(writeEndTime - writeStartTime);
                        } else {
                            this.avgWriteTime = Duration.ofNanos(
                                    (this.avgWriteTime.toNanos() + writeEndTime
                                            - writeStartTime) / 2);
                        }
                    }).ifFailure(response -> {
                        this.failCount += itemCount;
                        log.error(
                                "data write failed, http code: {}, message: {} , optional reason: {},  data info: {} ",
                                response.getStatus(), response.getStatusText(),
                                response.getBody(), body);
                        response.getParsingError().ifPresent(e -> {
                            log.error("original body: {}, parsing exception",
                                    e.getOriginalBody(), e);
                            throw e;
                        });
                    });
        }
    }
}
