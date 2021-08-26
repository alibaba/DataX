package com.alibaba.datax;

import java.io.Serializable;
import java.util.Map;

public abstract class BasePlugin implements Serializable {

    public abstract SAPlugin instance(Map<String,Object> param);

    public abstract static class SAPlugin{

        /**
         * 自定义处理逻辑
         * @param properties 当前行数据
         * @return 是否继续执行后续逻辑
         */
        public abstract boolean process(Map<String,Object> properties);

    }
}
