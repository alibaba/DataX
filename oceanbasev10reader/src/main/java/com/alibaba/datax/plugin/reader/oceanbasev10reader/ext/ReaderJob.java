package com.alibaba.datax.plugin.reader.oceanbasev10reader.ext;

import java.util.List;

import com.alibaba.datax.common.constant.CommonConstant;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.CommonRdbmsReader;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.datax.plugin.reader.oceanbasev10reader.util.ObReaderUtils;

public class ReaderJob extends CommonRdbmsReader.Job {

    public ReaderJob() {
        super(ObReaderUtils.DATABASE_TYPE);
    }

    @Override
    public List<Configuration> split(Configuration originalConfig, int adviceNumber) {
        List<Configuration> list = super.split(originalConfig, adviceNumber);
        for (Configuration config : list) {
            String jdbcUrl = config.getString(Key.JDBC_URL);
            String obRegionName = getObRegionName(jdbcUrl);
            config.set(CommonConstant.LOAD_BALANCE_RESOURCE_MARK, obRegionName);
        }
        return list;
    }

    private String getObRegionName(String jdbcUrl) {
        if (jdbcUrl.startsWith(Constant.OB10_SPLIT_STRING)) {
            String[] ss = jdbcUrl.split(Constant.OB10_SPLIT_STRING_PATTERN);
            if (ss.length >= 2) {
                String tenant = ss[1].trim();
                String[] sss = tenant.split(":");
                return sss[0];
            }
        }
        return null;
    }
}
