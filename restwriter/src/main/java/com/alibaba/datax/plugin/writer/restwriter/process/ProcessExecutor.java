package com.alibaba.datax.plugin.writer.restwriter.process;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.writer.restwriter.conf.Operation;
import com.alibaba.datax.plugin.writer.restwriter.conf.Process;

import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import kong.unirest.HttpMethod;
import kong.unirest.HttpRequest;
import kong.unirest.HttpRequestWithBody;
import kong.unirest.HttpResponse;
import kong.unirest.JsonObjectMapper;
import kong.unirest.ObjectMapper;
import kong.unirest.Unirest;
import kong.unirest.UnirestInstance;
import lombok.extern.slf4j.Slf4j;

import static com.alibaba.datax.plugin.writer.restwriter.RestWriterErrorCode.EXPRESSION_EVALUATE_FAILED_EXCEPTION;
import static com.alibaba.datax.plugin.writer.restwriter.RestWriterErrorCode.OPERATION_RESULT_ERROR_EXCEPTION;
import static com.alibaba.datax.plugin.writer.restwriter.RestWriterErrorCode.POSTPROCESS_OPERATION_ERROR;
import static com.alibaba.datax.plugin.writer.restwriter.RestWriterErrorCode.PREPROCESS_OPERATION_ERROR;
import static com.alibaba.datax.plugin.writer.restwriter.process.ProcessCategory.PREPROCESS;
import static java.util.Objects.nonNull;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.ForkJoinPool.defaultForkJoinWorkerThreadFactory;
import static kong.unirest.ContentType.APPLICATION_JSON;
import static kong.unirest.HeaderNames.CONTENT_TYPE;
import static kong.unirest.HttpMethod.GET;
import static kong.unirest.HttpStatus.MULTIPLE_CHOICE;
import static org.apache.commons.collections4.CollectionUtils.isNotEmpty;
import static org.apache.commons.collections4.MapUtils.emptyIfNull;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

/**
 * @name: zhangyongxiang
 * @author: zhangyongxiang@baidu.com
 **/
@Slf4j
public class ProcessExecutor {
    
    private final UnirestInstance unirest;
    
    private final Executor executor;
    
    private final ObjectMapper json;
    
    private final ExpressionParser expressionParser;
    
    public ProcessExecutor() {
        this(new ForkJoinPool(Runtime.getRuntime().availableProcessors(),
                defaultForkJoinWorkerThreadFactory, null, true));
    }
    
    public ProcessExecutor(final Executor executor) {
        this.executor = executor;
        this.json = new JsonObjectMapper();
        this.expressionParser = new SpelExpressionParser();
        this.unirest = Unirest.spawnInstance();
        this.unirest.config().addShutdownHook(true);
        this.unirest.config().verifySsl(false);
        this.unirest.config().automaticRetries(false);
        this.unirest.config()
                .connectTimeout((int) Duration.ofHours(1).toMillis());
        this.unirest.config()
                .socketTimeout((int) Duration.ofHours(1).toMillis());
    }
    
    public void execute(final Process process) {
        if (nonNull(process) && isNotEmpty(process.getOperations())) {
            if (process.isConcurrent() && process.getOperations().size() > 1) {
                CompletableFuture.allOf(process
                        .getOperations().stream().map(
                                operation -> CompletableFuture.runAsync(
                                        () -> executeWithRetry(operation,
                                                process.getCategory()),
                                        this.executor))
                        .toArray(CompletableFuture[]::new)).exceptionally(e -> {
                            if (process.getCategory() == PREPROCESS) {
                                throw DataXException.asDataXException(
                                        PREPROCESS_OPERATION_ERROR,
                                        e.getMessage(), e);
                            } else {
                                throw DataXException.asDataXException(
                                        POSTPROCESS_OPERATION_ERROR,
                                        e.getMessage(), e);
                            }
                        }).join();
            } else {
                process.getOperations()
                        .forEach(operation -> executeWithRetry(operation,
                                process.getCategory()));
            }
        }
    }
    
    public HttpResponse<String> executeWithRetry(final Operation operation,
            final ProcessCategory category) {
        if (operation.getMaxRetries() > 1) {
            final RetryPolicy<HttpResponse<String>> retryPolicy = RetryPolicy
                    .<HttpResponse<String>>builder()
                    .withMaxRetries(operation.getMaxRetries())
                    .handleResultIf(
                            response -> response.getStatus() >= MULTIPLE_CHOICE)
                    .withBackoff(1, 5, ChronoUnit.SECONDS)
                    .onFailedAttempt(e -> log.error(
                            "{} operation execute failed, attempt execution times: {},"
                                    + " possible result response code: {},  possible result response body: {}",
                            category, e.getAttemptCount(),
                            ofNullable(e.getLastResult())
                                    .map(HttpResponse::getStatusText)
                                    .orElse(null),
                            ofNullable(e.getLastResult())
                                    .map(HttpResponse::getBody).orElse(null),
                            e.getLastException()))
                    .onRetry(e -> log.warn(
                            "{} operation failure #{}th retrying.", category,
                            e.getAttemptCount()))
                    .onRetriesExceeded(e -> log.error(
                            "fail to execute operation {}. max retries exceeded. cause: {}",
                            operation,
                            nonNull(e.getException())
                                    ? e.getException().getMessage()
                                    : e.getResult().getStatusText(),
                            e.getException()))
                    .build();
            return Failsafe.with(retryPolicy).get(
                    () -> executeWithExpectedResponse(operation, category));
        } else {
            return executeWithExpectedResponse(operation, category);
        }
    }
    
    public HttpResponse<String> executeWithExpectedResponse(
            final Operation operation, final ProcessCategory category) {
        final HttpResponse<String> response = execute(operation, category);
        if (response.getHeaders().containsKey(CONTENT_TYPE)
                && response.getHeaders().get(CONTENT_TYPE).stream()
                        .anyMatch(contentType -> contentType
                                .contains(APPLICATION_JSON.getMimeType()))
                && isNotBlank(operation.getJsonExpression())) {
            if (isBlank(response.getBody())) {
                log.warn("response body is empty, operation: {}", operation);
                throw DataXException.asDataXException(
                        EXPRESSION_EVALUATE_FAILED_EXCEPTION,
                        String.format("operation %s return empty response body",
                                operation.getUrl()));
            }
            try {
                final Object bodyResponse = json.readValue(response.getBody(),
                        Map.class);
                if (operation.isDebug()) {
                    log.info(
                            "operation {} return response body: {}, deserialized value {}, class {}",
                            operation.getUrl(), operation.getBody(),
                            bodyResponse, bodyResponse.getClass().getName());
                }
                final StandardEvaluationContext context = new StandardEvaluationContext(
                        bodyResponse);
                context.addPropertyAccessor(new MapAccessor());
                if (!BooleanUtils.toBoolean(expressionParser
                        .parseExpression(operation.getJsonExpression())
                        .getValue(context, boolean.class))) {
                    throw DataXException.asDataXException(
                            OPERATION_RESULT_ERROR_EXCEPTION,
                            String.format(
                                    "operation return result is not right according the json expression,"
                                            + " result %s, expression: %s",
                                    bodyResponse,
                                    operation.getJsonExpression()));
                }
            } catch (final Exception e) {
                log.error(
                        "body {} can't be deserialized or can't be evaluated against json expression {}, operation: {}",
                        response.getBody(), operation.getJsonExpression(),
                        operation);
                throw DataXException.asDataXException(
                        EXPRESSION_EVALUATE_FAILED_EXCEPTION,
                        String.format(
                                "operation result can't be deserialized or evaluated failed,"
                                        + " result %s, expression: %s",
                                response.getBody(),
                                operation.getJsonExpression()),
                        e);
            }
        }
        return response;
    }
    
    /**
     * @param operation operations
     * @param category  operations category
     * @return response
     */
    public HttpResponse<String> execute(final Operation operation,
            final ProcessCategory category) {
        HttpRequestWithBody requestBuilder = this.unirest
                .request(operation.getMethod(), operation.getUrl());
        if (MapUtils.isNotEmpty(operation.getHeaders())) {
            for (final String header : operation.getHeaders().keySet()) {
                requestBuilder = requestBuilder.header(header,
                        operation.getHeaders().get(header));
            }
        }
        if (!emptyIfNull(operation.getHeaders()).containsKey(CONTENT_TYPE)) {
            requestBuilder = requestBuilder.header(CONTENT_TYPE,
                    APPLICATION_JSON.getMimeType());
        }
        HttpRequest<?> request = requestBuilder;
        if (HttpMethod.valueOf(operation.getMethod()) != GET
                && isNotBlank(operation.getBody())) {
            if (operation.isBase64()) {
                request = requestBuilder
                        .body(Base64.getDecoder().decode(operation.getBody()));
            } else {
                request = requestBuilder.body(operation.getBody());
            }
            if (operation.isDebug()) {
                log.info(
                        "request {} method {} has body: {}, base64 encoded?{}, decoded body: {}",
                        operation.getUrl(), operation.getMethod(),
                        operation.getBody(), operation.isBase64(),
                        operation.isBase64()
                                ? Base64.getDecoder()
                                        .decode(operation.getBody())
                                : EMPTY);
            }
        }
        final long startTime = System.nanoTime();
        return request.asString().ifSuccess(response -> log.info(
                "operation {} category: {} execute successfully,response: {}, body: {}, consume time: {}",
                operation.getUrl(), category, response.getStatusText(),
                response.getBody(),
                Duration.ofNanos(System.nanoTime() - startTime)))
                .ifFailure(response -> {
                    response.getParsingError().ifPresent(e -> {
                        log.error(
                                "operation {} category: {} execute failed, original body: {}, parsing exception",
                                operation.getUrl(),
                                category.name().toLowerCase(),
                                e.getOriginalBody(), e);
                        throw new OperationExecutionFailException(String.format(
                                "operation %s category: %s execute failed",
                                operation.getUrl(),
                                category.name().toLowerCase()), e);
                    });
                    log.error("operation {} category: {} execute failed, "
                            + "http code: {}, message: {} , optional reason: {}",
                            operation.getUrl(), category.name().toLowerCase(),
                            response.getStatus(), response.getStatusText(),
                            response.getBody());
                    throw new OperationExecutionFailException(String.format(
                            "operation %s category: %s http execute failed, http code: %d, message: %s, optional reason: %s",
                            operation.getUrl(), category.name().toLowerCase(),
                            response.getStatus(), response.getStatusText(),
                            response.getBody()), null);
                });
    }
}
