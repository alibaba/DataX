package com.alibaba.datax.plugin.writer.otswriter.model;

import java.util.List;

public interface OTSTaskManagerInterface {
    public void execute(List<OTSLine> lines) throws Exception;

    public void close() throws Exception;
}
