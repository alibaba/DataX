package com.alibaba.datax.plugin.writer.otswriter;

import com.alibaba.datax.common.util.Configuration;

import java.util.List;

public interface IOtsWriterMasterProxy {

    public void init(Configuration param) throws Exception;

    public void close();

    public List<Configuration> split(int mandatoryNumber);


}
