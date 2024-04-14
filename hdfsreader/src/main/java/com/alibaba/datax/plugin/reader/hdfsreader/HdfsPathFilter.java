package com.alibaba.datax.plugin.reader.hdfsreader;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * Created by wmy on 16/11/29.
 */
public class HdfsPathFilter implements PathFilter {

    private String regex = null;

    public HdfsPathFilter(String regex) {
        this.regex = regex;
    }

    @Override
    public boolean accept(Path path) {
        return regex != null ? path.getName().matches(regex) : true;
    }
}
