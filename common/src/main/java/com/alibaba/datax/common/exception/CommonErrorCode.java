package com.alibaba.datax.common.exception;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 *
 */
public enum CommonErrorCode implements ErrorCode {

    CONFIG_ERROR("Common-00", "您提供的配置文件存在错误信息，请检查您的作业配置 ."),
    CONVERT_NOT_SUPPORT("Common-01", "同步数据出现业务脏数据情况，数据类型转换错误 ."),
    CONVERT_OVER_FLOW("Common-02", "同步数据出现业务脏数据情况，数据类型转换溢出 ."),
    RETRY_FAIL("Common-10", "方法调用多次仍旧失败 ."),
    RUNTIME_ERROR("Common-11", "运行时内部调用错误 ."),
    HOOK_INTERNAL_ERROR("Common-12", "Hook运行错误 ."),
    SHUT_DOWN_TASK("Common-20", "Task收到了shutdown指令，为failover做准备"),
    WAIT_TIME_EXCEED("Common-21", "等待时间超出范围"),
    TASK_HUNG_EXPIRED("Common-22", "任务hung住，Expired");

    private final String code;

    private final String describe;

    private CommonErrorCode(String code, String describe) {
        this.code = code;
        this.describe = describe;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.describe;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Describe:[%s]", this.code,
                this.describe);
    }

}
