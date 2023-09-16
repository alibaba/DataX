package com.alibaba.datax.plugin.reader.otsreader;

import java.util.List;

import com.alibaba.datax.common.util.Configuration;

public interface IOtsReaderMasterProxy {

    public void init(Configuration param) throws Exception;

    public List<Configuration> split(int num) throws Exception;

    public void close();

}
