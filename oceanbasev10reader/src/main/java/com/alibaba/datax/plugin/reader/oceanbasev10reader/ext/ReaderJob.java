package com.alibaba.datax.plugin.reader.oceanbasev10reader.ext;

import java.util.List;

import com.alibaba.datax.common.constant.CommonConstant;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.CommonRdbmsReader;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.datax.plugin.reader.oceanbasev10reader.OceanBaseReader;
import com.alibaba.datax.plugin.reader.oceanbasev10reader.util.ObReaderUtils;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ReaderJob extends CommonRdbmsReader.Job {
    private Logger LOG = LoggerFactory.getLogger(OceanBaseReader.Task.class);

    public ReaderJob() {
        super(ObReaderUtils.databaseType);
    }

    @Override
    public void init(Configuration originalConfig) {
        //将config中的column和table中的关键字进行转义
        List<String> columns = originalConfig.getList(Key.COLUMN, String.class);
        ObReaderUtils.escapeDatabaseKeywords(columns);
        originalConfig.set(Key.COLUMN, columns);

        List<JSONObject> conns = originalConfig.getList(com.alibaba.datax.plugin.rdbms.reader.Constant.CONN_MARK, JSONObject.class);
        for (int i = 0; i < conns.size(); i++) {
            JSONObject conn = conns.get(i);
            Configuration connConfig = Configuration.from(conn.toString());
            List<String> tables = connConfig.getList(Key.TABLE, String.class);
            ObReaderUtils.escapeDatabaseKeywords(tables);
            originalConfig.set(String.format("%s[%d].%s", com.alibaba.datax.plugin.rdbms.reader.Constant.CONN_MARK, i, Key.TABLE), tables);
        }
        super.init(originalConfig);
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
