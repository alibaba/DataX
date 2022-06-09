package com.alibaba.datax.plugin.reader.odpsreader.util;

import java.util.List;

public class UserConfiguredPartitionClassification {

    //包含/*query*/的partition, 例如: /*query*/ dt>=20170101 and dt<= 20170109
    private List<String> userConfiguredHintPartition;

    //不包含/*query*/的partition, 例如: dt=20170101 或者 dt=201701*
    private List<String> userConfiguredNormalPartition;

    //是否包含hint的partition
    private boolean isIncludeHintPartition;

    public List<String> getUserConfiguredHintPartition() {
        return userConfiguredHintPartition;
    }

    public void setUserConfiguredHintPartition(List<String> userConfiguredHintPartition) {
        this.userConfiguredHintPartition = userConfiguredHintPartition;
    }

    public List<String> getUserConfiguredNormalPartition() {
        return userConfiguredNormalPartition;
    }

    public void setUserConfiguredNormalPartition(List<String> userConfiguredNormalPartition) {
        this.userConfiguredNormalPartition = userConfiguredNormalPartition;
    }

    public boolean isIncludeHintPartition() {
        return isIncludeHintPartition;
    }

    public void setIncludeHintPartition(boolean includeHintPartition) {
        isIncludeHintPartition = includeHintPartition;
    }
}
