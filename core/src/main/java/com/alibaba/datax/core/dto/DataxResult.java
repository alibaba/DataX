package com.alibaba.datax.core.dto;

import com.alibaba.datax.common.util.Configuration;

import java.util.Map;
import java.util.StringJoiner;

/**
 * @author Jarod.Kong
 */
public class DataxResult {
    private Configuration configuration;

    private Map<String, String> jobLog;

    public Configuration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    public Map<String, String> getJobLog() {
        return jobLog;
    }

    public void setJobLog(Map<String, String> jobLog) {
        this.jobLog = jobLog;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", DataxResult.class.getSimpleName() + "[", "]")
                .add("configuration=" + configuration)
                .add("jobLog=" + jobLog)
                .toString();
    }
}
