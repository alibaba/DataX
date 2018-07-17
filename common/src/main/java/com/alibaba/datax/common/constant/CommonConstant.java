package com.alibaba.datax.common.constant;

public final class CommonConstant {
    /**
     * 用于插件对自身 split 的每个 task 标识其使用的资源，以告知core 对 reader/writer split 之后的 task 进行拼接时需要根据资源标签进行更有意义的 shuffle 操作
     */
    public static String LOAD_BALANCE_RESOURCE_MARK = "loadBalanceResourceMark";

}
