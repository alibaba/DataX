/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */
 
package com.alibaba.datax.plugin.writer.otswriter;
 
/**
 * 表示来自开放结构化数据服务（Open Table Service，OTS）的错误代码。
 *
 */
public interface OTSErrorCode {
    /**
     * 用户身份验证失败。
     */
    static final String AUTHORIZATION_FAILURE = "OTSAuthFailed";
 
    /**
     * 服务器内部错误。
     */
    static final String INTERNAL_SERVER_ERROR = "OTSInternalServerError";
 
    /**
     * 参数错误。
     */
    static final String INVALID_PARAMETER = "OTSParameterInvalid";
     
    /**
     * 整个请求过大。
     */
    static final String REQUEST_TOO_LARGE = "OTSRequestBodyTooLarge";
     
    /**
     * 客户端请求超时。
     */
    static final String REQUEST_TIMEOUT = "OTSRequestTimeout";
 
    /**
     * 用户的配额已经用满。
     */
    static final String QUOTA_EXHAUSTED = "OTSQuotaExhausted";
 
    /**
     * 内部服务器发生failover，导致表的部分分区不可服务。
     */
    static final String PARTITION_UNAVAILABLE = "OTSPartitionUnavailable";
     
    /**
     * 表刚被创建还无法立马提供服务。
     */
    static final String TABLE_NOT_READY = "OTSTableNotReady";
 
    /**
     * 请求的表不存在。
     */
    static final String OBJECT_NOT_EXIST = "OTSObjectNotExist";
 
    /**
     * 请求创建的表已经存在。
     */
    static final String OBJECT_ALREADY_EXIST = "OTSObjectAlreadyExist";
 
    /**
     * 多个并发的请求写同一行数据，导致冲突。
     */
    static final String ROW_OPEARTION_CONFLICT = "OTSRowOperationConflict";
 
    /**
     * 主键不匹配。
     */
    static final String INVALID_PK = "OTSInvalidPK";
 
    /**
     * 读写能力调整过于频繁。
     */
    static final String TOO_FREQUENT_RESERVED_THROUGHPUT_ADJUSTMENT = "OTSTooFrequentReservedThroughputAdjustment";
 
    /**
     * 该行总列数超出限制。
     */
    static final String OUT_OF_COLUMN_COUNT_LIMIT = "OTSOutOfColumnCountLimit";
 
    /**
     * 该行所有列数据大小总和超出限制。
     */
    static final String OUT_OF_ROW_SIZE_LIMIT = "OTSOutOfRowSizeLimit";
 
    /**
     * 剩余预留读写能力不足。
     */
    static final String NOT_ENOUGH_CAPACITY_UNIT = "OTSNotEnoughCapacityUnit";
 
    /**
     * 预查条件检查失败。
     */
    static final String CONDITION_CHECK_FAIL = "OTSConditionCheckFail";
 
    /**
     * 在OTS内部操作超时。
     */
    static final String STORAGE_TIMEOUT = "OTSTimeout";
 
    /**
     * 在OTS内部有服务器不可访问。
     */
    static final String SERVER_UNAVAILABLE = "OTSServerUnavailable";
 
    /**
     * OTS内部服务器繁忙。
     */
    static final String SERVER_BUSY = "OTSServerBusy";
    
}