package com.alibaba.datax.plugin.rdbms.reader.util;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.Constant;
import com.alibaba.datax.plugin.rdbms.reader.Key;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by liuyi on 15/9/18.
 */
public class HintUtil {
    private static final Logger LOG = LoggerFactory.getLogger(ReaderSplitUtil.class);

    private static DataBaseType dataBaseType;
    private static String username;
    private static String password;
    private static Pattern tablePattern;
    private static String hintExpression;

    public static void initHintConf(DataBaseType type, Configuration configuration){
        dataBaseType = type;
        username = configuration.getString(Key.USERNAME);
        password = configuration.getString(Key.PASSWORD);
        String hint = configuration.getString(Key.HINT);
        if(StringUtils.isNotBlank(hint)){
            String[] tablePatternAndHint = hint.split("#");
            if(tablePatternAndHint.length==1){
                tablePattern = Pattern.compile(".*");
                hintExpression = tablePatternAndHint[0];
            }else{
                tablePattern = Pattern.compile(tablePatternAndHint[0]);
                hintExpression = tablePatternAndHint[1];
            }
        }
    }

    public static String buildQueryColumn(String jdbcUrl, String table, String column){
        try{
            if(tablePattern != null && DataBaseType.Oracle.equals(dataBaseType)) {
                Matcher m = tablePattern.matcher(table);
                if(m.find()){
                    String[] tableStr = table.split("\\.");
                    String tableWithoutSchema = tableStr[tableStr.length-1];
                    String finalHint = hintExpression.replaceAll(Constant.TABLE_NAME_PLACEHOLDER, tableWithoutSchema);
                    //主库不并发读取
                    if(finalHint.indexOf("parallel") > 0 && DBUtil.isOracleMaster(jdbcUrl, username, password)){
                        LOG.info("master:{} will not use hint:{}", jdbcUrl, finalHint);
                    }else{
                        LOG.info("table:{} use hint:{}.", table, finalHint);
                        return finalHint + column;
                    }
                }
            }
        } catch (Exception e){
            LOG.warn("match hint exception, will not use hint", e);
        }
        return column;
    }

}
